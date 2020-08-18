package gos

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
)

/*
Scans a struct, converting fields tagged with `db` into a sequence of named
`SqlArgs`. The input must be a struct or a struct pointer. A nil pointer is
fine and produces a nil result. Panics on other inputs. Treats an embedded
struct as part of the enclosing struct.
*/
func StructSqlArgs(input interface{}) SqlArgs {
	rval := reflect.ValueOf(input)
	if isRvalNil(rval) {
		return nil
	}

	var args SqlArgs

	err := traverseStructRvalueFields(rval, func(structRval reflect.Value, fieldIndex int) error {
		sfield := structRval.Type().Field(fieldIndex)
		colName := structFieldColumnName(sfield)
		if colName == "" {
			return nil
		}
		args = append(args, SqlArg{
			Name:  colName,
			Value: structRval.Field(fieldIndex).Interface(),
		})
		return nil
	})

	if err != nil {
		panic(err)
	}

	return args
}

/*
Sequence of named SQL arguments with utility methods for query building.
Usually obtained by calling `StructSqlArgs()`.
*/
type SqlArgs []SqlArg

/*
Returns the argument names.
*/
func (self SqlArgs) Names() []string {
	var names []string
	for _, arg := range self {
		names = append(names, arg.Name)
	}
	return names
}

/*
Returns the argument values.
*/
func (self SqlArgs) Values() []interface{} {
	var values []interface{}
	for _, arg := range self {
		values = append(values, arg.Value)
	}
	return values
}

/*
Returns comma-separated argument names, suitable for a `select` clause. Example:

	args := gos.SqlArgs{{"one", 10}, {"two", 20}}

	fmt.Sprintf(`select %v`, args.NamesString())

	// Output:
	`select "one", "two"`
*/
func (self SqlArgs) NamesString() string {
	var buf []byte
	for i, arg := range self {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = stringAppendQuoted(buf, arg.Name)
	}
	return bytesToMutableString(buf)
}

/*
Returns parameter placeholders in the Postgres style `$N`, comma-separated,
suitable for a `values` clause. Example:

	args := gos.SqlArgs{{"one", 10}, {"two", 20}}

	fmt.Sprintf(`values (%v)`, args.ValuesString())

	// Output:
	`values ($1, $2)`
*/

func (self SqlArgs) ValuesString() string {
	var buf []byte
	for i := range self {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(i+1), 10)
	}
	return bytesToMutableString(buf)
}

/*
Returns the string of names and values suitable for an `insert` clause. Example:

	args := gos.SqlArgs{{"one", 10}, {"two", 20}}

	fmt.Sprintf(`insert into some_table %v`, args.NamesAndValuesString())

	// Output:
	`insert into some_table ("one", "two") values ($1, $2)`
*/
func (self SqlArgs) NamesAndValuesString() string {
	if len(self) == 0 {
		return "default values"
	}
	return fmt.Sprintf("(%v) values (%v)", self.NamesString(), self.ValuesString())
}

/*
Returns the string of assignments suitable for an `update set` clause. Example:

	args := gos.SqlArgs{{"one", 10}, {"two", 20}}

	fmt.Sprintf(`update some_table set %v`, args.AssignmentsString())

	// Output:
	`update some_table set "one" = $1, "two" = $2`
*/
func (self SqlArgs) AssignmentsString() string {
	var buf []byte
	for i, arg := range self {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = stringAppendQuoted(buf, arg.Name)
		buf = append(buf, " = $"...)
		buf = strconv.AppendInt(buf, int64(i+1), 10)
	}
	return bytesToMutableString(buf)
}

/*
Returns the string of conditions suitable for a `where` or `join` clause.
Example:

	args := gos.SqlArgs{{"one", 10}, {"two", 20}}

	fmt.Sprintf(`select * from some_table where %v`, args.ConditionsString())

	// Output (formatted for readability):
	`
	select * from some_table
	where
		"one" is not distinct from $1 and
		"two" is not distinct from $2
	`
*/
func (self SqlArgs) ConditionsString() string {
	if len(self) == 0 {
		return "true"
	}

	var buf []byte

	for i, arg := range self {
		if i > 0 {
			buf = append(buf, " and "...)
		}
		buf = stringAppendQuoted(buf, arg.Name)
		buf = append(buf, " is not distinct from $"...)
		buf = strconv.AppendInt(buf, int64(i+1), 10)
	}

	return bytesToMutableString(buf)
}

/*
Returns true if at least one argument satisfies the predicate function. Example:

  args.Some(SqlArg.IsNil)
*/
func (self SqlArgs) Some(fun func(SqlArg) bool) bool {
	for _, arg := range self {
		if fun != nil && fun(arg) {
			return true
		}
	}
	return false
}

/*
Returns true if every argument satisfies the predicate function. Example:

  args.Every(SqlArg.IsNil)
*/
func (self SqlArgs) Every(fun func(SqlArg) bool) bool {
	for _, arg := range self {
		if fun == nil || !fun(arg) {
			return false
		}
	}
	return true
}

// Same as `sql.NamedArg`, with additional methods. See `SqlArgs`.
type SqlArg struct {
	Name  string
	Value interface{}
}

func (self SqlArg) IsValid() bool {
	return columnNameRegexp.MatchString(self.Name)
}

var columnNameRegexp = regexp.MustCompile(`^(?:\w+(?:\.\w+)*|"\w+(?:\.\w+)*")$`)

func (self SqlArg) IsNil() bool {
	return isNil(self.Value)
}
