## Overview

**Go** ↔︎ **S**QL: tool for generating SQL queries and decoding results into Go structs.

**Not an ORM**, and should be used **instead** of an ORM. Expressly designed to let you **write plain SQL**.

Key features:

* Decodes SQL records into Go structs.
* Supports nested records/structs.
* Supports nilable nested records/structs in outer joins.
* Supports generating named SQL arguments from structs.
* Query builder oriented towards plain SQL. (No DSL in Go.)

See the full documentation at https://godoc.org/github.com/mitranim/gos.

## Differences from [jmoiron/sqlx](https://github.com/jmoiron/sqlx)

Gos is somewhat similar to [jmoiron/sqlx](https://github.com/jmoiron/sqlx). Key differences:

* Supports null records in outer joins, as nested struct pointers.
* Selects fields explicitly, by reflecting on the output struct. This allows _you_ to write `select *`, but if the struct is lacking some of the fields, the DB will optimize them out of the query.
* Simpler API, does not wrap `database/sql`.
* Explicit field-column mapping, no hidden renaming.
* Has only one tiny dependency (most deps in `go.mod` are test-only).
* Can convert structs into named SQL arguments.
* Has a simple query builder.
* ... probably more

## Features Under Consideration

* Short column aliases.

Like many similar libraries, when selecting fields for nested records, Gos relies on column aliases like `"column_name.column_name"`. With enough nesting, they can become too long. At the time of writing, Postgres 12 has an identifier length limit of 63 and will _silently truncate_ the remainder, causing queries to fail, or worse. One solution is shorter aliases, such as `"1.2.3"` from struct field indexes. We still want to support long alises for manually-written queries, which means the library would have to support _both_ alias types, which could potentially cause collisions. Unclear what's the best approach.

* Streaming API.

An API for scanning rows one-by-one, like `database/sql.Rows.Scan()` but for structs. This would allow streaming and might be useful when processing a very large amount of rows. Scanning into structs involves reflecting on the type and generating a spec, and this must be done only once; the API would have to be designed to make it seamless for the user. One option is to wrap `sql.Rows` into an object that provides `.Next()` and `.Scan()`, generates the output spec on the first call to `.Scan()` and stores it, and on subsequent calls ensures that the same destination type was provided.

## Changelog

### 0.1.1

First tagged release. Adds `SqlArgs` and `SqlQuery` for query building.

## Contributing

Issues and pull requests are welcome! The development setup is simple:

```sh
git clone https://github.com/mitranim/gos
cd gos
go test
```

Tests currently require a local instance of Postgres on the default port. They create a separate "database", make no persistent changes, and drop it at the end.

## License

https://en.wikipedia.org/wiki/WTFPL

## Misc

I'm receptive to suggestions. If this library _almost_ satisfies you but needs changes, open an issue or chat me up. Contacts: https://mitranim.com/#contacts
