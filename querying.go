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
Executes an SQL query and prepares a `Scanner` that can decode individual rows
into structs or scalars. A `Scanner` is used similarly to `*sql.Rows`, but
automatically maps columns to struct fields. Just like `*sql.Rows`, this avoids
buffering all results in memory, which is especially useful for large sets.

The returned scanner MUST be closed after finishing.

Example:

	scan, err := QueryScanner(ctx, conn, query, args)
	panic(err)
	defer scan.Close()

	for scan.Next() {
		var result ResultType
		err := scan.Scan(&result)
		panic(err)
	}
*/
func QueryScanner(ctx context.Context, conn Queryer, query string, args []interface{}) (Scanner, error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, Err{While: `querying rows`, Cause: err}
	}
	return &scanner{Rows: rows}, nil
}

/*
Shortcut for scanning columns into the destination, which may be one of:

	* Single scalar.
	* Slice of scalars.
	* Single struct.
	* Slice of structs.

When the output is a slice, the query should use a small `limit`. When
processing a large data set, prefer `QueryScanner()` to scan rows one-by-one
without buffering the result.

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
using the sibling package "github.com/mitranim/sqlb".
*/
func Query(ctx context.Context, conn Queryer, dest interface{}, query string, args []interface{}) error {
	err := validateDest(dest)
	if err != nil {
		return err
	}

	scan, err := QueryScanner(ctx, conn, query, args)
	if err != nil {
		return err
	}
	defer scan.Close()

	if rtypeDerefKind(reflect.TypeOf(dest)) == reflect.Slice {
		return scanMany(dest, scan)
	}
	return scanOne(dest, scan)
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

func scanMany(dest interface{}, scan Scanner) error {
	rval := reflect.ValueOf(dest)
	sliceRval := refut.RvalDerefAlloc(rval)
	truncateSliceRval(sliceRval)

	elemRtype := rtypeDerefElem(rval.Type())

	for scan.Next() {
		ptrRval := reflect.New(elemRtype)

		err := scan.Scan(ptrRval.Interface())
		if err != nil {
			return err
		}

		sliceRval.Set(reflect.Append(sliceRval, ptrRval.Elem()))
	}

	return nil
}

func scanOne(dest interface{}, scan Scanner) error {
	if !scan.Next() {
		err := scan.Err()
		if err != nil {
			return Err{While: `preparing row`, Cause: err}
		}
		return ErrNoRows.while(`preparing row`)
	}

	err := scan.Scan(dest)
	if err != nil {
		return err
	}

	if scan.Next() {
		return ErrMultipleRows.while(`verifying row count`)
	}
	return nil
}

type scanner struct {
	*sql.Rows
	rtype reflect.Type
	spec  *tDestSpec
}

func (self *scanner) Scan(dest interface{}) error {
	rval := reflect.ValueOf(dest)

	err := validateDest(dest)
	if err != nil {
		return err
	}

	rtype := rval.Type()

	if self.rtype == nil {
		self.rtype = rtype
	} else {
		err := validateMatchingDestType(self.rtype, rtype)
		if err != nil {
			return err
		}
	}

	if isRtypeStructNonScannable(rtype) {
		return self.scanStruct(rval)
	}
	return self.scanScalar(dest)
}

func (self *scanner) scanStruct(rval reflect.Value) error {
	if self.spec == nil {
		spec, err := prepareDestSpec(self.Rows, self.rtype)
		if err != nil {
			return err
		}
		self.spec = spec
	}

	state, err := prepareDecodeState(self.Rows, self.spec)
	if err != nil {
		return err
	}

	err = self.Rows.Scan(state.colPtrs...)
	if err != nil {
		return ErrScan.because(err)
	}

	return traverseDecode(rval, self.spec, state, &self.spec.typeSpec, nil)
}

func (self *scanner) scanScalar(dest interface{}) error {
	err := self.Rows.Scan(dest)
	if err != nil {
		return ErrScan.because(err)
	}
	return nil
}

func prepareDestSpec(rows *sql.Rows, rtype reflect.Type) (*tDestSpec, error) {
	if rtype == nil || rtype.Kind() != reflect.Ptr || rtypeDerefKind(rtype) != reflect.Struct {
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

		if isRtypeStructNonScannable(fieldRtype) {
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

		if isRtypeStructNonScannable(fieldRtype) {
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

func validateDest(dest interface{}) error {
	rval := reflect.ValueOf(dest)
	if rval.IsValid() && rval.Kind() == reflect.Ptr && !rval.IsNil() {
		return nil
	}
	return ErrInvalidDest.because(fmt.Errorf(`destination must be a non-nil pointer, received %#v`, dest))
}

func validateMatchingDestType(expected, found reflect.Type) error {
	if expected != found {
		return ErrInvalidDest.because(fmt.Errorf(`destination must be of type %v, received %v`, expected, found))
	}
	return nil
}
