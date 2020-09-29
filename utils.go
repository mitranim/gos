package gos

import (
	"context"
	"database/sql"
	"reflect"
	"time"
	"unsafe"

	"github.com/mitranim/refut"
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

func copyIntSlice(src []int) []int {
	if src == nil {
		return nil
	}
	out := make([]int, len(src), len(src))
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
Allocation-free conversion. Reinterprets a byte slice as a string. Borrowed from
the standard library. Reasonably safe. Should not be used when the underlying
byte array is volatile, for example when it's part of a scratch buffer during
SQL scanning.
*/
func bytesToMutableString(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
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
