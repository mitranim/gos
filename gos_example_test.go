package gos_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mitranim/gos"
)

func ExampleQuery() {
	type External struct {
		Id   string `db:"id"`
		Name string `db:"name"`
	}

	type Internal struct {
		Id   string `db:"id"`
		Name string `db:"name"`
	}

	// Step 1: generate query.

	var result []External

	query := fmt.Sprintf(`
select %v from (
	select
		external.*,
		internal as internal
	from
		external
		cross join internal
) as _
`, gos.Cols(result))

	/**
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

	var ctx context.Context
	var conn *sql.Tx
	err := gos.Query(ctx, conn, &result, query, nil)
	if err != nil {
		panic(err)
	}
}

func ExampleCols() {
	type External struct {
		Id   string `db:"id"`
		Name string `db:"name"`
	}

	type Internal struct {
		Id   string `db:"id"`
		Name string `db:"name"`
	}

	fmt.Println(gos.Cols(External{}))

	/**
	Formatted here for readability:

	"id",
	"name",
	("internal")."id"   as "internal.id",
	("internal")."name" as "internal.name"
	*/
}
