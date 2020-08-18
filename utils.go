package gos

import (
	"context"
	"database/sql"
	"reflect"
	"regexp"
	"strconv"
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

func copyIntSlice(vals []int) []int {
	out := make([]int, len(vals), len(vals))
	copy(out, vals)
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

func structRtypeSqlIdents(rtype reflect.Type) []sqlIdent {
	var idents []sqlIdent

	err := refut.TraverseStructRtype(rtype, func(sfield reflect.StructField, _ []int) error {
		colName := sfieldColumnName(sfield)
		if colName == "" {
			return nil
		}

		fieldRtype := refut.RtypeDeref(sfield.Type)
		if fieldRtype.Kind() == reflect.Struct && !isScannableRtype(fieldRtype) {
			idents = append(idents, sqlIdent{
				name:   colName,
				idents: structRtypeSqlIdents(fieldRtype),
			})
			return nil
		}

		idents = append(idents, sqlIdent{name: colName})
		return nil
	})
	if err != nil {
		panic(err)
	}

	return idents
}

type sqlIdent struct {
	name   string
	idents []sqlIdent
}

func (self sqlIdent) selectString() string {
	return bytesToMutableString(self.appendSelect(nil, nil))
}

func (self sqlIdent) appendSelect(buf []byte, path []sqlIdent) []byte {
	/**
	If the ident doesn't have a name, it's just a collection of other idents,
	which are considered to be at the "top level". If the ident has a name, it's
	considered to "contain" the other idents.
	*/
	if len(self.idents) > 0 {
		if self.name != "" {
			path = append(path, self)
		}
		for _, ident := range self.idents {
			buf = ident.appendSelect(buf, path)
		}
		return buf
	}

	if self.name == "" {
		return buf
	}

	if len(buf) > 0 {
		buf = append(buf, `, `...)
	}

	if len(path) == 0 {
		buf = self.appendAlias(buf, nil)
	} else {
		buf = self.appendPath(buf, path)
		buf = append(buf, ` as `...)
		buf = self.appendAlias(buf, path)
	}

	return buf
}

func (self sqlIdent) appendPath(buf []byte, path []sqlIdent) []byte {
	for i, ident := range path {
		if i == 0 {
			buf = appendDelimited(buf, `("`, ident.name, `")`)
		} else {
			buf = appendDelimited(buf, `"`, ident.name, `"`)
		}
		buf = append(buf, `.`...)
	}
	buf = appendDelimited(buf, `"`, self.name, `"`)
	return buf
}

func (self sqlIdent) appendAlias(buf []byte, path []sqlIdent) []byte {
	buf = append(buf, `"`...)
	for _, ident := range path {
		buf = append(buf, ident.name...)
		buf = append(buf, `.`...)
	}
	buf = append(buf, self.name...)
	buf = append(buf, `"`...)
	return buf
}

func appendDelimited(buf []byte, prefix, infix, suffix string) []byte {
	buf = append(buf, prefix...)
	buf = append(buf, infix...)
	buf = append(buf, suffix...)
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
TODO: consider validating that the column name doesn't contain double quotes. We
might return an error, or panic.
*/
func sfieldColumnName(sfield reflect.StructField) string {
	return refut.TagIdent(sfield.Tag.Get("db"))
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
