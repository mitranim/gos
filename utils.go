package gos

import (
	"context"
	"database/sql"
	"io"
	"reflect"
	"time"

	"github.com/mitranim/refut"
)

/*
Database connection required by `Query`. Satisfied by `*sql.DB`, `*sql.Tx`, may
be satisfied by other types.
*/
type QueryExecer interface {
	Queryer
	Execer
}

/*
Database connection required by `QueryScanner`. Satisfied by `*sql.DB`,
`*sql.Tx`, may be satisfied by other types.
*/
type Queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

/*
Subset of `QueryExecer`. Satisfied by `*sql.DB`, `*sql.Tx`, may be satisfied by
other types.
*/
type Execer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

/*
Decodes individual SQL rows in a streaming fashion. Returned by `QueryScanner()`.
*/
type Scanner interface {
	// Same as `(*sql.Rows).Close`. MUST be called at the end.
	io.Closer

	// Same as `(*sql.Rows).Next`.
	Next() bool

	// Same as `(*sql.Rows).Err`.
	Err() error

	// Decodes the current row into the output. For technical reasons, the output
	// type is cached on the first call and must be the same for every call.
	Scan(interface{}) error
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

func isRtypeScannable(rtype reflect.Type) bool {
	return rtype != nil &&
		(rtype == timeRtype || reflect.PtrTo(rtype).Implements(sqlScannerRtype))
}

// WTB better name.
func isRtypeStructNonScannable(rtype reflect.Type) bool {
	rtype = refut.RtypeDeref(rtype)
	return rtype != nil && rtype.Kind() == reflect.Struct && !isRtypeScannable(rtype)
}

func copyIntSlice(src []int) []int {
	if src == nil {
		return nil
	}
	out := make([]int, len(src))
	copy(out, src)
	return out
}

func isNilableOrHasNilableNonRootAncestor(fieldSpec *tFieldSpec) bool {
	for fieldSpec != nil {
		if refut.IsRkindNilable(fieldSpec.typeSpec.rtype.Kind()) {
			return true
		}
		fieldSpec = fieldSpec.parentFieldSpec
	}
	return false
}

/*
TODO: consider validating that the column name doesn't contain double quotes. We
might return an error, or panic.
*/
func sfieldColumnName(sfield reflect.StructField) string {
	return refut.TagIdent(sfield.Tag.Get("db"))
}

/*
Truncates the length, keeping the available capacity. The input must be a slice.
Safe to call on a nil slice.
*/
func truncateSliceRval(rval reflect.Value) {
	rval.SetLen(0)
}

func rtypeDerefKind(rtype reflect.Type) reflect.Kind {
	rtype = refut.RtypeDeref(rtype)
	if rtype == nil {
		return reflect.Invalid
	}
	return rtype.Kind()
}

func rtypeDerefElem(rtype reflect.Type) reflect.Type {
	return refut.RtypeDeref(rtype).Elem()
}

func rvalZero(rval reflect.Value) {
	rval.Set(reflect.Zero(rval.Type()))
}

/*
Adapted from `refut.RvalFieldByPathAlloc` but specialized for zeroing rather
than allocating.
*/
func rvalZeroAtPath(rval reflect.Value, path []int) {
	for len(path) > 0 {
		for rval.Kind() == reflect.Ptr {
			if rval.IsNil() {
				return
			}
			rval = rval.Elem()
		}

		if rval.Kind() != reflect.Struct {
			panic(refut.ErrInvalidValue)
		}

		rval = rval.Field(path[0])
		path = path[1:]
	}

	rvalZero(rval)
}

// Assumes that types of `src` and `tar` match.
func set(tar, src reflect.Value) {
	if tar.Kind() == reflect.Ptr {
		if tar.IsNil() {
			tar.Set(src)
			return
		}

		if src.IsNil() {
			rvalZero(tar)
			return
		}

		tar.Elem().Set(src.Elem())
		return
	}

	tar.Set(src)
}
