package gos

import (
	"context"
	"database/sql"
	"reflect"
	"regexp"
	"strconv"
	"time"
	"unsafe"
)

/*
Database connection passed to `Query()`. Satisfied by `*sql.DB`, `*sql.Tx`,
may be satisfied by other types.
*/
type Queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

/*
Database connection passed to `SqlQuery.Exec`. Satisfied by `*sql.DB`,
`*sql.Tx`, may be satisfied by other types.
*/
type Execer interface {
	ExecContext(context.Context, string, ...interface{}) (*sql.Result, error)
}

func stringIndex(strs []string, str string) int {
	for i := range strs {
		if strs[i] == str {
			return i
		}
	}
	return -1
}

var timeRtype = reflect.TypeOf(time.Time{})
var sqlScannerRtype = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func isScannableRtype(rtype reflect.Type) bool {
	return rtype != nil &&
		(rtype == timeRtype || reflect.PtrTo(rtype).Implements(sqlScannerRtype))
}

func isNonNilPointer(rval reflect.Value) bool {
	return rval.IsValid() && rval.Kind() == reflect.Ptr && !rval.IsNil()
}

func copyIntSlice(vals []int) []int {
	out := make([]int, len(vals), len(vals))
	copy(out, vals)
	return out
}

func isNilableOrHasNilableNonRootAncestor(fieldSpec *tFieldSpec) bool {
	for fieldSpec != nil {
		if isNilableRkind(fieldSpec.typeSpec.rtype.Kind()) {
			return true
		}
		fieldSpec = fieldSpec.parentFieldSpec
	}
	return false
}

func structRtypeSqlIdents(structRtype reflect.Type) []sqlIdent {
	var idents []sqlIdent

	traverseStructRtypeFields(structRtype, func(sfield reflect.StructField) {
		colName := structFieldColumnName(sfield)
		if colName == "" {
			return
		}

		fieldRtype := derefRtype(sfield.Type)
		if fieldRtype.Kind() == reflect.Struct && !isScannableRtype(fieldRtype) {
			idents = append(idents, sqlIdent{
				name:   colName,
				idents: structRtypeSqlIdents(fieldRtype),
			})
			return
		}

		idents = append(idents, sqlIdent{name: colName})
		return
	})

	return idents
}

type sqlIdent struct {
	name   string
	idents []sqlIdent
}

func (self sqlIdent) selectString() string {
	return bytesToStringAlloc(self.appendSelect(nil, nil))
}

func (self sqlIdent) appendSelect(buf []byte, path []string) []byte {
	if len(self.idents) == 0 {
		if self.name == "" {
			return buf
		}
		if len(buf) > 0 {
			buf = append(buf, ", "...)
		}
		if len(path) == 0 {
			buf = appendIdentAlias(buf, path, self.name)
		} else {
			buf = appendIdentPath(buf, path, self.name)
			buf = append(buf, " as "...)
			buf = appendIdentAlias(buf, path, self.name)
		}
		return buf
	}

	if self.name != "" {
		path = append(path, self.name)
	}
	for _, ident := range self.idents {
		buf = ident.appendSelect(buf, path)
	}
	return buf
}

func appendIdentPath(buf []byte, path []string, ident string) []byte {
	for i, name := range path {
		if i == 0 {
			buf = append(buf, '(', '"')
			buf = append(buf, name...)
			buf = append(buf, '"', ')')
		} else {
			buf = append(buf, '"')
			buf = append(buf, name...)
			buf = append(buf, '"')
		}
		buf = append(buf, '.')
	}
	buf = append(buf, '"')
	buf = append(buf, ident...)
	buf = append(buf, '"')
	return buf
}

func appendIdentAlias(buf []byte, path []string, ident string) []byte {
	buf = append(buf, '"')
	for _, name := range path {
		buf = append(buf, name...)
		buf = append(buf, '.')
	}
	buf = append(buf, ident...)
	buf = append(buf, '"')
	return buf
}

// Potential gotcha: the input must not contain quotes.
func stringAppendQuoted(buf []byte, input string) []byte {
	buf = append(buf, '"')
	buf = append(buf, input...)
	buf = append(buf, '"')
	return buf
}

// Self-reminder about non-free conversions.
func bytesToStringAlloc(bytes []byte) string { return string(bytes) }

// Self-reminder about non-free conversions.
func stringToBytesAlloc(input string) []byte { return []byte(input) }

/*
Allocation-free conversion. Reinterprets a byte slice as a string. Borrowed from
the standard library. Reasonably safe. Should not be used when the underlying
byte array is volatile, for example when it's part of a scratch buffer during
SQL scanning.
*/
func bytesToMutableString(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}

/*
TODO: consider passing the entire path from the root value rather than the field
index. This is more expensive but allows the caller to choose to allocate deeply
nested fields on demand.
*/
func traverseStructRvalueFields(rval reflect.Value, fun func(reflect.Value, int) error) error {
	rval = derefRval(rval)
	rtype := rval.Type()
	if rtype.Kind() != reflect.Struct {
		return Err{
			Code:  ErrCodeInvalidInput,
			While: `traversing struct fields`,
			Cause: fmt.Errorf("expected a struct, got a %q", rtype),
		}
	}

	for i := 0; i < rtype.NumField(); i++ {
		sfield := rtype.Field(i)
		if !isStructFieldPublic(sfield) {
			continue
		}

		/**
		If this is an embedded struct, traverse its fields as if they're in the
		parent struct.
		*/
		if sfield.Anonymous && derefRtype(sfield.Type).Kind() == reflect.Struct {
			err := traverseStructRvalueFields(rval.Field(i), fun)
			if err != nil {
				return err
			}
			continue
		}

		err := fun(rval, i)
		if err != nil {
			return err
		}
	}

	return nil
}

func traverseStructRtypeFields(rtype reflect.Type, fun func(sfield reflect.StructField)) {
	rtype = derefRtype(rtype)
	if rtype == nil || rtype.Kind() != reflect.Struct {
		panic(Err{
			Code:  ErrCodeInvalidInput,
			While: `traversing struct type fields`,
			Cause: fmt.Errorf("expected a struct type, got a %q", rtype),
		})
	}

	for i := 0; i < rtype.NumField(); i++ {
		sfield := rtype.Field(i)
		if !isStructFieldPublic(sfield) {
			continue
		}

		/**
		If this is an embedded struct, traverse its fields as if they're in the
		parent struct.
		*/
		if sfield.Anonymous && derefRtype(sfield.Type).Kind() == reflect.Struct {
			traverseStructRtypeFields(sfield.Type, fun)
			continue
		}

		fun(sfield)
	}
}

func derefRtype(rtype reflect.Type) reflect.Type {
	for rtype != nil && rtype.Kind() == reflect.Ptr {
		rtype = rtype.Elem()
	}
	return rtype
}

/*
Recursively dereferences a `reflect.Value` until it's not a pointer type. Panics
if any pointer in the sequence is nil.
*/
func derefRval(rval reflect.Value) reflect.Value {
	for rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	return rval
}

/*
Derefs the provided value until it's no longer a pointer, allocating as
necessary. Returns a non-pointer value. The input value must be settable or a
non-nil pointer, otherwise this causes a panic.
*/
func derefAllocRval(rval reflect.Value) reflect.Value {
	for rval.Kind() == reflect.Ptr {
		if rval.IsNil() {
			rval.Set(reflect.New(rval.Type().Elem()))
		}
		rval = rval.Elem()
	}
	return rval
}

/*
Finds or allocates an rval at the given struct field path, returning the
resulting reflect value. If the starting value is settable, the resulting value
should also be settable. Ditto if the starting value is a non-nil pointer and
the path is not empty.

Assumes that every type on the path, starting with the root, is a struct type or
an arbitrarily nested struct pointer type. Panics if the assumption doesn't
hold.
*/
func derefAllocStructRvalAt(rval reflect.Value, path []int) reflect.Value {
	for _, i := range path {
		rval = derefAllocRval(rval)
		rval = rval.Field(i)
	}
	return rval
}

func isStructFieldPublic(sfield reflect.StructField) bool {
	return sfield.PkgPath == ""
}

func structFieldColumnName(sfield reflect.StructField) string {
	tag := sfield.Tag.Get("db")
	if tag == "-" {
		return ""
	}
	return tag
}

func isNilableRkind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return true
	default:
		return false
	}
}

func isRvalNil(rval reflect.Value) bool {
	return !rval.IsValid() || isNilableRkind(rval.Kind()) && rval.IsNil()
}

/*
Difference from `value == nil`: returns `true` if the input is a non-nil
`interface{}` whose value is a nil pointer, slice, etc.
*/
func isNil(value interface{}) bool {
	return value == nil || isRvalNil(reflect.ValueOf(value))
}

/*
Renumerates "$1" param placeholders by adding the given offset.

TODO: better parser that ignores $N inside string literals. The parser should be
used for both this and `sqlAppendNamed`.
*/
func sqlRenumerateOrdinalParams(query string, offset int) string {
	return postgresPositionalParamRegexp.ReplaceAllStringFunc(query, func(match string) string {
		num, err := strconv.Atoi(match[1:])
		if err != nil {
			panic(err)
		}
		return "$" + strconv.Itoa(num+offset)
	})
}

var postgresPositionalParamRegexp = regexp.MustCompile(`\$\d+\b`)
