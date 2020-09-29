package gos

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitranim/refut"
)

/*
Scans columns into the destination, which may be one of:

	* Single scalar.
	* Slice of scalars.
	* Single struct.
	* Slice of structs.

If the destination is a non-slice, there must be exactly one row. Less or more
will result in an error. If the destination is a struct, this will decode
columns into struct fields, following the rules outlined above in the package
overview.

The `select` part of the query should follow the common convention for selecting
nested fields, see below.

	type Inner struct {
		InnerValue string `db:"inner_value"`
	}
	type OuterValue struct {
		Inner      Inner `db:"inner"`
		OuterValue string `db:"outer_value"`
	}

The query should have:

	select
		outer_value         as "outer_value",
		(inner).inner_value as "inner.inner_value"

The easiest way to generate the query correctly is by calling `sqlb.Cols(dest)`,
using the sibling package https://godoc.org/github.com/mitranim/sqlb.
*/
func Query(ctx context.Context, conn Queryer, dest interface{}, query string, args []interface{}) error {
	rval := reflect.ValueOf(dest)
	if !isNonNilPointer(rval) {
		return ErrInvalidDest.because(fmt.Errorf(`destination must be a non-nil pointer, received %#v`, dest))
	}

	rtype := refut.RtypeDeref(rval.Type())

	if rtype.Kind() == reflect.Slice {
		elemRtype := refut.RtypeDeref(rtype.Elem())
		if elemRtype.Kind() == reflect.Struct && !isScannableRtype(elemRtype) {
			return queryStructs(ctx, conn, rval, query, args)
		}
		return queryScalars(ctx, conn, rval, query, args)
	}

	if rtype.Kind() == reflect.Struct && !isScannableRtype(rtype) {
		return queryStruct(ctx, conn, rval, query, args)
	}
	return queryScalar(ctx, conn, dest, query, args)
}

/* Internal */

const expectedStructDepth = 8

type tDestSpec struct {
	colNames  []string
	colRtypes map[string]reflect.Type
	typeSpec  tTypeSpec
}

type tTypeSpec struct {
	rtype      reflect.Type
	fieldSpecs []tFieldSpec
}

type tFieldSpec struct {
	parentFieldSpec *tFieldSpec
	typeSpec        tTypeSpec
	fieldIndex      int
	fieldPath       []int // Relative to root struct.
	colName         string
	uniqColAlias    string
	colIndex        int // Must be initialized to -1.
	sfield          reflect.StructField
}

type tDecodeState struct {
	colPtrs []interface{}
}

/*
The destination must be a pointer to a non-scannable struct.
*/
func queryStruct(ctx context.Context, conn Queryer, rval reflect.Value, query string, args []interface{}) error {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return Err{While: `querying rows`, Cause: err}
	}
	defer rows.Close()

	spec, err := prepareDestSpec(rows, rval.Type())
	if err != nil {
		return err
	}

	state, err := prepareDecodeState(rows, spec)
	if err != nil {
		return err
	}

	if !rows.Next() {
		err := rows.Err()
		if err != nil {
			return Err{While: `preparing row`, Cause: err}
		}
		return ErrNoRows.while(`preparing row`)
	}

	err = rows.Scan(state.colPtrs...)
	if err != nil {
		return Err{While: `scanning row`, Cause: err}
	}

	err = traverseDecode(rval, spec, state, &spec.typeSpec, nil)
	if err != nil {
		return err
	}

	if rows.Next() {
		return ErrMultipleRows.while(`verifying row count`)
	}

	return nil
}

/*
The destination must be a pointer to a slice of non-scannable structs or
pointers to those structs.
*/
func queryStructs(ctx context.Context, conn Queryer, rval reflect.Value, query string, args []interface{}) error {
	elemRtype := refut.RtypeDeref(rval.Type()).Elem()

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return Err{While: `querying rows`, Cause: err}
	}
	defer rows.Close()

	spec, err := prepareDestSpec(rows, reflect.PtrTo(elemRtype))
	if err != nil {
		return err
	}

	sliceRval := refut.RvalDerefAlloc(rval)
	truncateSliceRval(sliceRval)

	for rows.Next() {
		state, err := prepareDecodeState(rows, spec)
		if err != nil {
			return err
		}

		err = rows.Scan(state.colPtrs...)
		if err != nil {
			return Err{While: `scanning row`, Cause: err}
		}

		elemPtrRval := reflect.New(elemRtype)

		err = traverseDecode(elemPtrRval, spec, state, &spec.typeSpec, nil)
		if err != nil {
			return err
		}

		sliceRval.Set(reflect.Append(sliceRval, elemPtrRval.Elem()))
	}

	return nil
}

func queryScalar(ctx context.Context, conn Queryer, dest interface{}, query string, args []interface{}) error {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return Err{While: `querying rows`, Cause: err}
	}
	defer rows.Close()

	if !rows.Next() {
		err := rows.Err()
		if err != nil {
			return Err{While: `preparing row`, Cause: err}
		}
		return ErrNoRows.while(`preparing row`)
	}

	err = rows.Scan(dest)
	if err != nil {
		return Err{While: `scanning row`, Cause: err}
	}

	if rows.Next() {
		return ErrMultipleRows.while(`verifying row count`)
	}

	return nil
}

/*
The destination must be a pointer to a slice of scannables or primitives.
*/
func queryScalars(ctx context.Context, conn Queryer, rval reflect.Value, query string, args []interface{}) error {
	elemRtype := refut.RtypeDeref(rval.Type()).Elem()

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return Err{While: `querying rows`, Cause: err}
	}
	defer rows.Close()

	sliceRval := refut.RvalDerefAlloc(rval)
	truncateSliceRval(sliceRval)

	for rows.Next() {
		elemPtrRval := reflect.New(elemRtype)

		err = rows.Scan(elemPtrRval.Interface())
		if err != nil {
			return Err{While: `scanning row`, Cause: err}
		}

		sliceRval.Set(reflect.Append(sliceRval, elemPtrRval.Elem()))
	}

	return nil
}

func prepareDestSpec(rows *sql.Rows, rtype reflect.Type) (*tDestSpec, error) {
	if rtype == nil || rtype.Kind() != reflect.Ptr || refut.RtypeDeref(rtype).Kind() != reflect.Struct {
		return nil, Err{
			Code:  ErrCodeInvalidDest,
			While: `preparing destination spec`,
			Cause: fmt.Errorf(`expected destination type to be a struct pointer, got %q`, rtype),
		}
	}

	colNames, err := rows.Columns()
	if err != nil {
		return nil, Err{While: `getting columns`, Cause: err}
	}

	spec := &tDestSpec{
		typeSpec:  tTypeSpec{rtype: rtype},
		colNames:  colNames,
		colRtypes: map[string]reflect.Type{},
	}

	colPath := make([]string, 0, expectedStructDepth)
	fieldPath := make([]int, 0, expectedStructDepth)
	err = traverseMakeSpec(spec, &spec.typeSpec, nil, colPath, fieldPath)
	if err != nil {
		return nil, err
	}

	for _, colName := range colNames {
		if spec.colRtypes[colName] == nil {
			return nil, Err{
				Code:  ErrCodeNoColDest,
				While: `preparing destination spec`,
				Cause: fmt.Errorf(`column %q doesn't have a matching destination in type %q`, colName, rtype),
			}
		}
	}

	return spec, nil
}

func prepareDecodeState(rows *sql.Rows, spec *tDestSpec) (*tDecodeState, error) {
	colPtrs := make([]interface{}, 0, len(spec.colNames))
	for _, colName := range spec.colNames {
		if spec.colRtypes[colName] == nil {
			return nil, Err{
				Code:  ErrCodeNoColDest,
				While: `preparing decode state`,
				Cause: fmt.Errorf(`column %q doesn't have a matching destination in type %q`,
					colName, spec.typeSpec.rtype),
			}
		}
		colPtrs = append(colPtrs, reflect.New(reflect.PtrTo(spec.colRtypes[colName])).Interface())
	}
	return &tDecodeState{colPtrs: colPtrs}, nil
}

func traverseMakeSpec(
	spec *tDestSpec, typeSpec *tTypeSpec, parentFieldSpec *tFieldSpec, colPath []string, fieldPath []int,
) error {
	rtypeElem := refut.RtypeDeref(typeSpec.rtype)
	typeSpec.fieldSpecs = make([]tFieldSpec, rtypeElem.NumField())

	for i := 0; i < rtypeElem.NumField(); i++ {
		sfield := rtypeElem.Field(i)
		fieldRtype := refut.RtypeDeref(sfield.Type)
		fieldPath := append(fieldPath, i)

		fieldSpec := &typeSpec.fieldSpecs[i]
		*fieldSpec = tFieldSpec{
			parentFieldSpec: parentFieldSpec,
			typeSpec:        tTypeSpec{rtype: sfield.Type},
			fieldIndex:      i,
			fieldPath:       copyIntSlice(fieldPath),
			colIndex:        -1,
			sfield:          sfield,
		}

		if !refut.IsSfieldExported(sfield) {
			continue
		}

		if sfield.Anonymous && fieldRtype.Kind() == reflect.Struct {
			err := traverseMakeSpec(spec, &fieldSpec.typeSpec, fieldSpec, colPath, fieldPath)
			if err != nil {
				return err
			}
			continue
		}

		fieldSpec.colName = sfieldColumnName(sfield)
		if fieldSpec.colName == "" {
			continue
		}

		colPath := append(colPath, fieldSpec.colName)
		fieldSpec.uniqColAlias = strings.Join(colPath, ".")
		fieldSpec.colIndex = stringIndex(spec.colNames, fieldSpec.uniqColAlias)

		if spec.colRtypes[fieldSpec.uniqColAlias] != nil {
			return Err{
				Code:  ErrCodeRedundantCol,
				While: `preparing destination spec`,
				Cause: fmt.Errorf(`redundant occurrence of column %q`, fieldSpec.uniqColAlias),
			}
		}
		spec.colRtypes[fieldSpec.uniqColAlias] = sfield.Type

		if fieldRtype.Kind() == reflect.Struct && !isScannableRtype(fieldRtype) {
			err := traverseMakeSpec(spec, &fieldSpec.typeSpec, fieldSpec, colPath, fieldPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func traverseDecode(
	rootRval reflect.Value, spec *tDestSpec, state *tDecodeState, typeSpec *tTypeSpec, fieldSpec *tFieldSpec,
) error {
	everyColValueIsNil := true

	for i := range typeSpec.fieldSpecs {
		fieldSpec := &typeSpec.fieldSpecs[i]
		sfield := fieldSpec.sfield
		fieldRtype := refut.RtypeDeref(sfield.Type)

		if !refut.IsSfieldExported(sfield) {
			continue
		}

		if sfield.Anonymous && fieldRtype.Kind() == reflect.Struct {
			err := traverseDecode(rootRval, spec, state, &fieldSpec.typeSpec, fieldSpec)
			if err != nil {
				return err
			}
			continue
		}

		if fieldSpec.colName == "" {
			continue
		}

		if fieldRtype.Kind() == reflect.Struct && !isScannableRtype(fieldRtype) {
			err := traverseDecode(rootRval, spec, state, &fieldSpec.typeSpec, fieldSpec)
			if err != nil {
				return err
			}
			continue
		}

		if !(fieldSpec.colIndex >= 0) {
			continue
		}

		colRval := reflect.ValueOf(state.colPtrs[fieldSpec.colIndex]).Elem()
		if !colRval.IsNil() {
			everyColValueIsNil = false
		}
	}

	isNested := fieldSpec != nil
	if everyColValueIsNil && isNested && isNilableOrHasNilableNonRootAncestor(fieldSpec) {
		return nil
	}

	for _, fieldSpec := range typeSpec.fieldSpecs {
		if !(fieldSpec.colIndex >= 0) {
			continue
		}

		sfield := fieldSpec.sfield
		colRval := reflect.ValueOf(state.colPtrs[fieldSpec.colIndex]).Elem()

		if colRval.IsNil() {
			if refut.IsRkindNilable(sfield.Type.Kind()) {
				continue
			}

			fieldRval := refut.RvalFieldByPathAlloc(rootRval, fieldSpec.fieldPath)
			scanner, ok := fieldRval.Addr().Interface().(sql.Scanner)
			if ok {
				err := scanner.Scan(nil)
				if err != nil {
					return Err{Code: ErrCodeScan, While: `scanning into field`, Cause: err}
				}
				continue
			}

			return Err{
				Code:  ErrCodeNull,
				While: `decoding into struct`,
				Cause: fmt.Errorf(`type %q at field %q of struct %q is not nilable, but corresponding column %q was null`,
					sfield.Type, sfield.Name, typeSpec.rtype, fieldSpec.uniqColAlias),
			}
		}

		fieldRval := refut.RvalFieldByPathAlloc(rootRval, fieldSpec.fieldPath)
		fieldRval.Set(colRval.Elem())
	}

	return nil
}
