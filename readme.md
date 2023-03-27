**Moved to https://github.com/mitranim/gg**. This repo is usable but frozen.

## Overview

**Go** ↔︎ **S**QL: tool for decoding results into Go structs. Supports streaming.

**Not an ORM**, and should be used **instead** of an ORM, in combination with a simple query builder (see below).

Key features:

* Decodes SQL records into Go structs.
* Supports nested records/structs.
* Supports nilable nested records/structs in outer joins.
* Supports streaming.

See the full documentation at https://pkg.go.dev/github.com/mitranim/gos.

See the sibling library https://pkg.go.dev/github.com/mitranim/sqlb: a simple query builder that supports scanning structs into named arguments.

## Differences from [jmoiron/sqlx](https://github.com/jmoiron/sqlx)

Gos is somewhat similar to [jmoiron/sqlx](https://github.com/jmoiron/sqlx). Key differences:

* Supports null records in outer joins, as nested struct pointers.
* Selects fields explicitly, by reflecting on the output struct. This allows _you_ to write `select *`, but if the struct is lacking some of the fields, the DB will optimize them out of the query.
* Simpler API, does not wrap `database/sql`.
* Explicit field-column mapping, no hidden renaming.
* Has only one tiny dependency (most deps in `go.mod` are test-only).
* ... probably more

## Features Under Consideration

* Short column aliases.

Like many similar libraries, when selecting fields for nested records, Gos relies on column aliases like `"column_name.column_name"`. With enough nesting, they can become too long. At the time of writing, Postgres 12 has an identifier length limit of 63 and will _silently truncate_ the remainder, causing queries to fail, or worse. One solution is shorter aliases, such as `"1.2.3"` from struct field indexes. We still want to support long alises for manually-written queries, which means the library would have to support _both_ alias types, which could potentially cause collisions. Unclear what's the best approach.

## Changelog

### 0.1.10

Improved how `Query` and `Scanner` handle previously-existing values in the output, especially in regards to pointers.

When a row contains `null`, the corresponding Go value is now zeroed rather than ignored. The old non-zeroing behavior was aligned with `encoding/json`. The new behavior diverges from it.

When a row contains non-`null` and the corresponding Go value is a non-nil pointer, the value is written to the pointer's target, without replacing the pointer.

### 0.1.9

`Query` allows nil output, using `conn.ExecContext` to discard the result.

### 0.1.8

Support streaming via `QueryScanner` and `Scanner`.

### 0.1.7

Dependency update.

### 0.1.6

Breaking: moved query generation utils into https://pkg.go.dev/github.com/mitranim/sqlb.

### 0.1.5

Fixed an oversight in `queryStruct` and `queryScalar` that could lead to shadowing DB errors with `ErrNoRows` in some edge cases.

### 0.1.4

Added `SqlQuery.QueryAppend`.

### 0.1.3

Changed the license to Unlicense.

### 0.1.2

Breaking changes in named args utils for symmetry with `database/sql`.

* Added `Named()`.
* Renamed `SqlArg -> NamedArg`.
* Renamed `SqlArgs -> NamedArgs`.
* Renamed `StructSqlArgs -> StructNamedArgs`.

Also moved some reflection-related utils to a [tiny dependency](https://github.com/mitranim/refut).

### 0.1.1

First tagged release. Added `SqlArgs` and `SqlQuery` for query building.

## Contributing

Issues and pull requests are welcome! The development setup is simple:

```sh
git clone https://github.com/mitranim/gos
cd gos
go test
```

Tests currently require a local instance of Postgres on the default port. They create a separate "database", make no persistent changes, and drop it at the end.

## License

https://unlicense.org

## Misc

I'm receptive to suggestions. If this library _almost_ satisfies you but needs changes, open an issue or chat me up. Contacts: https://mitranim.com/#contacts
