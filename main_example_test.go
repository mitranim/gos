package gos_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mitranim/gos"
	"github.com/mitranim/sqlb"
)

func ExampleQuery() {
	type Internal struct {
		Id   string `db:"id"`
		Name string `db:"name"`
	}

	type External struct {
		Id       string   `db:"id"`
		Name     string   `db:"name"`
		Internal Internal `db:"internal"`
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
`, sqlb.Cols(result))

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
