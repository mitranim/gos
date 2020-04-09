## Overview

**Go** ↔︎ **S**QL: tool for mapping between Go and SQL. Features:

* Decodes SQL records into Go structs
* Supports nested records/structs
* Supports nilable nested records/structs in outer joins
* Helps to generate queries from structs

**This is not an ORM**, and should be used **instead** of an ORM. This tool is expressly designed to let you **write plain SQL**.

See the full documentation at https://godoc.org/github.com/mitranim/gos.

## Differences from [jmoiron/sqlx](https://github.com/jmoiron/sqlx)

Gos is very similar to [jmoiron/sqlx](https://github.com/jmoiron/sqlx). Key differences:

* Supports null records in outer joins, as nested struct pointers
* Selects fields explicitly without relying on `select *`
* Much simpler API, does not wrap `database/sql`
* Explicit field-column mapping, no hidden renaming
* No special utilities for named parameters
* Depends only on the standard library (the `go.mod` dependencies are test-only)

## Usage Example

See the full documentation at https://godoc.org/github.com/mitranim/gos.

```go
type External struct {
  Id    string `db:"id"`
  Name  string `db:"name"`
}

type Internal struct {
  Id   string `db:"id"`
  Name string `db:"name"`
}

// Step 1: generate query.

var result []External

query := fmt.Sprintf(`select %v from (
  select
    external.*,
    internal as internal
  from
    external
    cross join internal
) as _
`, gos.Cols(result))

/*
Resulting query (formatted here for readability):

select
  "id",
  "name",
  ("internal")."id"   as "internal.id",
  ("internal")."name" as "internal.name"
from (
  ...
) as _
*/

// Step 2: use query.

err := gos.Query(ctx, conn, &result, query, nil)

fmt.Printf("%#v\n", result)
```

## Features Under Consideration

1. Utilities for insert and update queries.

The current API helps generate select queries from structs, but not insert or update queries. This needs to be rectified.

2. Short column aliases.

Like many similar libraries, when selecting fields for nested records, Gos relies on column aliases like `"column_name.column_name"`. With enough nesting, they can become too long. At the time of writing, Postgres 12 has an identifier length limit of 63 and will _silently truncate_ the remainder, causing queries to fail. One solution is shorter aliases, such as `"1.2.3"` from struct field indexes. We still want to support long alises for manually-written queries, which means the library would have to support _both_ alias types, which could potentially cause collisions. Unclear what's the best approach.

3. `select *`

Currently Gos requires you to generate queries with the help of `gos.Cols()`. It would be nice to just pass `select *` and have Gos automatically wrap the query into another `select`, listing the fields. Unfortunately not every query can be wrapped, but this can be provided as an additional function, alongside `Query()`.

4. Streaming API.

An API for scanning rows one-by-one, like `database/sql.Rows.Scan()` but for structs. This would allow streaming and might be useful when processing a very large amount of rows. Scanning into structs involves reflecting on the type and generating a spec, and this must be done only once; the API would have to be designed to make it seamless for the user. One option is to wrap `sql.Rows` into an object that provides `.Next()` and `.Scan()`, generates the output spec on the first call to `.Scan()` and stores it, and on subsequent calls ensures that the same destination type was provided.

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
