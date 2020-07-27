/*
Go SQL, tool for generating SQL queries and decoding results into Go structs.
NOT AN ORM, and should be used instead of an ORM. Expressly designed to help you
WRITE PLAIN SQL.

Key Features

• Decodes SQL records into Go structs. See `Query()`.

• Supports nested records/structs.

• Supports nilable nested records/structs in outer joins.

• Supports generating named SQL arguments from structs. See `StructSqlArgs()`.

• Query builder oriented towards plain SQL. (No DSL in Go.) See `SqlQuery`.

Struct Decoding Rules

When decoding a row into a struct, Gos observes the following rules.

1. Columns are matched to public struct fields whose `db` tag exactly matches
the column name. Private fields or fields without `db` are completely
ignored. Example:

	type Result struct {
		A string `db:"a"`
		B string // ignored: no `db` tag
		c string // ignored: private
	}

2. Fields of embedded structs are treated as part of the enclosing struct. For
example, these two definitions are completely equivalent:

	type Result struct {
		A string `db:"a"`
		Embedded
	}
	type Embedded struct {
		B string `db:"b"`
	}

	type Result struct {
		A string `db:"a"`
		B string `db:"b"`
	}

3. Fields of nested non-embedded structs are matched with columns whose aliases
look like `"outer_field.inner_field.innermost_field"` with arbitrary nesting.
Example:

	-- Query:
	select
		'one' as "outer_val",
		'two' as "inner.inner_val";

	// Go types:
	type Outer struct {
		OuterVal string `db:"outer_val"`
		Inner    Inner  `db:"inner"`
	}
	type Inner struct {
		InnerVal string `db:"inner_val"`
	}

4. If every column from a nested struct is null or missing, the entire nested
struct is considered null. If the field is not nilable (struct, not pointer
to struct), this will produce an error. Otherwise, the field is left nil and
not allocated. This convention is extremely useful for outer joins, where
nested records are often null. Example:

	-- Query:
	select
		'one' as "outer_val",
		null  as "inner.inner_val";

	// Go types:
	type Outer struct {
		OuterVal string `db:"outer_val"`
		Inner    *Inner `db:"inner"`
	}
	type Inner struct {
		InnerVal string `db:"inner_val"`
	}

	// Output:
	Outer{OuterVal: "one", Inner: nil}

Differences From sqlx

Gos is somewhat similar to https://github.com/jmoiron/sqlx. Key differences:

• Supports null records in outer joins, as nested struct pointers.

• Selects fields explicitly, by reflecting on structs, without relying on `select *`. (But _you_ can still write `select *`.)

• Simpler API, does not wrap `database/sql`.

• Explicit field-column mapping, no hidden renaming.

• Depends only on the standard library (the `go.mod` dependencies are test-only).

• Can convert structs into named SQL arguments.

• Has a simple query builder.

• ... probably more

Notes on Array Support

Gos doesn't specially support SQL arrays. Generally speaking, SQL arrays are
usable only for primitive types such as numbers or strings. Some databases, such
as Postgres, have their own implementations of multi-dimensional arrays, which
are non-standard and have so many quirks and limitations that it's more
practical to just use JSON. Arrays of primitives are already supported in
adapters such as "github.com/lib/pq", which are orthogonal to Gos and used in
combination with it.
*/
package gos
