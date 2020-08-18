package gos

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mitranim/refut"
)

/*
Tool for building SQL queries. Text-oriented; makes it easy to append arbitrary
SQL code while avoiding common mistakes.

Encapsulates arguments and automatically renumerates positional placeholders
when appending SQL code, making it easy to avoid argument interpolation (no SQL
injection) or accidental mis-numbering. See `SqlQuery.Append()`.

Supports named parameters. See `SqlQuery.AppendNamed()`.

Composable: supports interpolating a sub-query, automatically merging the
arguments and renumerating the parameter placeholders. See
`SqlQuery.QueryReplace()`.

Has shortcuts for `Cols()`, `Query()` and some other common functions. See
below.

Currently biased towards Postgres-style positional parameters of the form `$N`.
This can be rectified if there is enough demand; you can open an issue at
https://github.com/mitranim/gos/issues.
*/
type SqlQuery struct {
	Text string
	Args []interface{}
}

/*
Appends to the query, inserting whitespace if necessary. Appends additional args
to `SqlQuery.Args`; renumerates the positional parameters like `$1`, `$2` in the
appended chunk, offsetting them by the previous length of `SqlQuery.Args`.

For example, this:

	var query SqlQuery
	query.Append(`where true`)
	query.Append(`and one = $1`, 10)
	query.Append(`and two = $1`, 20) // Note the $1.

Becomes this:

	SqlQuery{
		Text: `where true and one = $1 and two = $2`,
		Args: []interface{}{10, 20},
	}

You can always numerate your parameters from `$1` without worrying about the
total count.

Also see `SqlQuery.MaybeAppend()` and `SqlQuery.AppendNamed()`.
*/
func (self *SqlQuery) Append(chunk string, args ...interface{}) {
	chunk = sqlRenumerateOrdinalParams(chunk, len(self.Args))
	if !isWhitespaceBetween(self.Text, chunk) {
		self.Text += "\n"
	}
	self.Text += chunk
	self.Args = append(self.Args, args...)
}

/*
Variant of `SqlQuery.Append` that only appends if the provided argument is not
nil.
*/
func (self *SqlQuery) MaybeAppend(chunk string, arg interface{}) {
	if !refut.IsNil(arg) {
		self.Append(chunk, arg)
	}
}

/*
Similar to `SqlQuery.Append()`, but uses named rather than positional
parameters. Parameters must have the form `:identifier`. This function replaces
them with positional placeholders of the form `$N`, appending the corresponding
values to `SqlQuery.Args`.

Panics on missing named parameters. Currently ignores unused parameters. This
should probably be rectified, ideally without allocating a book-keeping data
structure and without re-scanning `chunk` for each key in the named args.

TODO: replace regexp matching with a parser that detects comments and string
literals, and avoid replacing parameter placeholders inside those. Currently,
any parameters inside comments cause the query to fail, because we append
arguments that ends up being unused.

TODO: consider replacing all occurrences of each named arg with the same
positional placeholder and appending it to `SqlQuery.Args` only once.

TODO: validate that the keys in the dict match the allowed format of the
placeholders, and panic if they don't.

Example:

	var query SqlQuery
	query.AppendNamed(`select :value`, map[string]interface{}{"value": 10})

Resulting state:

	SqlQuery{Text: `select $1`, Args: []interface{}{10}}
*/
func (self *SqlQuery) AppendNamed(chunk string, namedArgs map[string]interface{}) {
	self.Text += namedParamRegexp.ReplaceAllStringFunc(chunk, func(match string) string {
		// Solution for the lack of negative lookbehind in the Go regexp
		// implementation.
		if match[:2] == "::" {
			return match
		}
		name := match[1:]

		arg, ok := namedArgs[name]
		if !ok {
			panic(Err{
				Code:  ErrCodeInvalidInput,
				While: `calling AppendNamed`,
				Cause: fmt.Errorf(`missing argument for the named parameter %q`, name),
			})
		}

		self.Args = append(self.Args, arg)
		return "$" + strconv.Itoa(len(self.Args))
	})
}

var namedParamRegexp = regexp.MustCompile(`:?:\w+\b`)

/*
Interpolates the other query inside itself, replacing every occurrence of the
given pattern. Renumerates positional parameters and appends the other query's
args to its own args.

TODO: panic if no occurrences of the pattern were found, and add a method called
`MaybeQueryReplace` which allows 0 to N matches.

Example:

	var outer SqlQuery
	outer.Append(`select * from some_table where col_one = $1 {{INNER}}`, 10)

	var inner SqlQuery
	inner.Append(`and col_two = $1`, 20)

	outer.QueryReplace(`{{INNER}}`, inner)

Resulting state of `outer`:

	SqlQuery{
		Text: `select * from some_table where col_one = $1 and col_two = $2`,
		Args: []interface{}{10, 20},
	}
*/
func (self *SqlQuery) QueryReplace(pattern string, other SqlQuery) {
	chunk := sqlRenumerateOrdinalParams(other.Text, len(self.Args))
	self.Text = strings.ReplaceAll(self.Text, pattern, chunk)
	self.Args = append(self.Args, other.Args...)
}

/*
Replaces the given string pattern inside the query.

TODO: panic if no occurrences of the pattern were found, and add a method called
`MaybeStringReplace` which allows 0 to N matches.

Example:

	var query SqlQuery
	query.Append(`select {{SELECT}} from some_table`)
	query.StringReplace(`{{SELECT}}`, `"id", "name"`)

Resulting state:

	SqlQuery{Text: `select "id", "name" from some_table`}

*/
func (self *SqlQuery) StringReplace(pattern string, chunk string) {
	self.Text = strings.ReplaceAll(self.Text, pattern, chunk)
}

/*
Wraps the query to select only the specified fields. Example:

	var query SqlQuery
	query.Append(`select * from some_table`)
	query.WrapSelect(`one, two`)

Resulting state is roughly equivalent to:

	SqlQuery{Text: `select one, two from some_table`}
*/
func (self *SqlQuery) WrapSelect(columns string) {
	self.Text = fmt.Sprintf(`with _ as (%v) select %v from _`, self.Text, columns)
}

/*
Wraps the query to select the fields derived by calling `Cols(dest)`. Example:

	var query SqlQuery
	query.Append(`select * from some_table`)

	var out struct{Id int64 `db:"id"`}
	query.WrapSelectCols(out)

Resulting state is roughly equivalent to:

	SqlQuery{Text: `select "id" from some_table`}

Also see `Cols()`.
*/
func (self *SqlQuery) WrapSelectCols(dest interface{}) {
	self.WrapSelect(Cols(dest))
}

/*
Makes a copy that doesn't share any mutable state with the original. Useful when
you want to "fork" a query and modify both versions.
*/
func (self SqlQuery) Copy() SqlQuery {
	args := self.Args
	if args != nil {
		self.Args = make([]interface{}, len(args), cap(args))
		copy(self.Args, args)
	}
	return self
}

// Shorter way to call `Query()`.
func (self SqlQuery) Query(ctx context.Context, conn Queryer, dest interface{}) error {
	return Query(ctx, conn, dest, self.Text, self.Args)
}

// Shorter way to call `sql.ExecContext`.
func (self SqlQuery) Exec(ctx context.Context, conn Execer) (*sql.Result, error) {
	return conn.ExecContext(ctx, self.Text, self.Args)
}

func isWhitespaceBetween(left string, right string) bool {
	return endWhitespaceRegexp.MatchString(left) || startWhitespaceRegexp.MatchString(right)
}

var startWhitespaceRegexp = regexp.MustCompile(`^[\n\s]`)
var endWhitespaceRegexp = regexp.MustCompile(`[\n\s]$`)
