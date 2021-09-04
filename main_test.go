package gos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/user"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	_ "github.com/lib/pq"
	"github.com/mitranim/sqlb"
)

const testDbName = `gos_test_db`

var testDb *sql.DB

func TestMain(m *testing.M) {
	os.Exit(runTestMain(m))
}

// This is a separate function to allow `defer` before `os.Exit`.
func runTestMain(m *testing.M) int {
	connParams := []string{
		`host=localhost`,
		`sslmode=disable`,
		`dbname=gos_test_db`,
	}

	/**
	Try using the current OS user as the Postgres user. Works on MacOS when
	Postgres is installed via Homebrew. Might fail in other configurations.
	*/
	usr, err := user.Current()
	if err != nil {
		panic(err)
	}
	connParams = append(connParams, `user=`+usr.Username)

	/**
	Create a test database and drop it at the end. Note that two concurrent
	instances of this test would conflict; we could create databases with random
	names to allow multiple instances of the test; seems unnecessary.
	*/
	dropDb(connParams, testDbName)
	err = createDb(connParams, testDbName)
	if err != nil {
		panic(err)
	}
	defer dropDb(connParams, testDbName)

	testConnParams := append(connParams, `dbname=`+testDbName)
	db, err := sql.Open("postgres", strings.Join(testConnParams, ` `))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	testDb = db

	return m.Run()
}

func TestQuery_scalar_basic(t *testing.T) {
	ctx, conn := testInit(t)

	var result string
	query := `select 'blah'`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := "blah"
	if expected != result {
		t.Fatalf(`expected %q, got %q`, expected, result)
	}
}

func TestQuery_scalar_non_nullable(t *testing.T) {
	ctx, conn := testInit(t)

	var result string
	query := `select null`
	err := Query(ctx, conn, &result, query, nil)
	/**
	Why this doesn't inspect the error: the error comes from `database/sql`;
	there's no programmatic API to detect its type. We return an `ErrNull` in
	some other scenarios.
	*/
	if err == nil {
		t.Fatalf(`expected scanning null into non-nullable scalar to produce an error`)
	}
}

func TestQuery_scalar_nullable(t *testing.T) {
	ctx, conn := testInit(t)

	var result *string
	query := `select 'blah'`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := "blah"
	if expected != *result {
		t.Fatalf(`expected %q, got %q`, expected, *result)
	}

	query = `select null`
	try(t, Query(ctx, conn, &result, query, nil))

	if result != nil {
		t.Fatalf(`expected selecting null to produce nil, got %q`, *result)
	}
}

func TestQuery_scalars_basic(t *testing.T) {
	ctx, conn := testInit(t)

	var results []string
	query := `select * from (values ('one'), ('two'), ('three')) as _`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []string{"one", "two", "three"}
	eq(t, expected, results)
}

func TestQuery_scalars_non_nullable(t *testing.T) {
	ctx, conn := testInit(t)

	var results []string
	query := `select * from (values ('one'), (null), ('three')) as _`
	err := Query(ctx, conn, &results, query, nil)
	/**
	Why this doesn't inspect the error: the error comes from `database/sql`;
	there's no programmatic API to detect its type. We return an `ErrNull` in
	some other scenarios.
	*/
	if err == nil {
		t.Fatalf(`expected scanning null into non-nullable scalar to produce an error`)
	}
}

func TestQuery_scalars_nullable(t *testing.T) {
	ctx, conn := testInit(t)

	var results []*string
	query := `select * from (values ('one'), (null), ('three')) as _`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []*string{strPtr("one"), nil, strPtr("three")}
	eq(t, expected, results)
}

// Verify that we treat `time.Time` as an atomic scannable rather than a struct.
func TestQuery_scalar_time(t *testing.T) {
	ctx, conn := testInit(t)

	var result time.Time
	query := `select '0001-01-01'::timestamp`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := timeMustParse(`0001-01-01T00:00:00Z`)
	if expected.UnixNano() != result.UnixNano() {
		t.Fatalf(`expected %v, got %v`, expected, result)
	}
}

// Verify that we treat `[]time.Time` as atomic scannables rather than structs.
func TestQuery_scalars_time(t *testing.T) {
	ctx, conn := testInit(t)

	var results []time.Time
	query := `select * from (values ('0001-01-01'::timestamp), ('0002-01-01'::timestamp)) as _`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []int64{
		timeMustParse(`0001-01-01T00:00:00Z`).UnixNano(),
		timeMustParse(`0002-01-01T00:00:00Z`).UnixNano(),
	}

	received := []int64{
		results[0].UnixNano(),
		results[1].UnixNano(),
	}

	eq(t, expected, received)
}

func TestQuery_scalar_scannable(t *testing.T) {
	ctx, conn := testInit(t)

	var result ScannableString
	query := `select 'blah'`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := "blah_scanned"
	received := string(result)
	if expected != received {
		t.Fatalf(`expected %q, got %q`, expected, received)
	}
}

func TestQuery_scalars_scannable(t *testing.T) {
	ctx, conn := testInit(t)

	var results []ScannableString
	query := `select * from (values ('one'), ('two'), ('three')) as _`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []string{"one_scanned", "two_scanned", "three_scanned"}
	received := *(*[]string)(unsafe.Pointer(&results))
	eq(t, expected, received)
}

/*
Verifies that when the output is a slice and the result set is empty, we
truncate the slice to empty, while retaining its nil or non-nil status, without
forcing it to become nil or non-nil. This allows users to define their own
empty semantics.
*/
func TestQuery_scalars_empty(t *testing.T) {
	ctx, conn := testInit(t)

	test := func(exp, res []string) {
		query := `select 'blah' where false`
		try(t, Query(ctx, conn, &res, query, nil))
		eq(t, exp, res)
	}

	test([]string(nil), []string(nil))
	test([]string{}, []string{})
	test([]string{}, []string{"blah"})
}

func TestQuery_struct_scannable(t *testing.T) {
	ctx, conn := testInit(t)

	var result ScannableStruct
	query := `select 'blah'`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := ScannableStruct{Value: "blah_scanned"}
	if expected != result {
		t.Fatalf(`expected %q, got %q`, expected, result)
	}
}

func TestQuery_structs_scannable(t *testing.T) {
	ctx, conn := testInit(t)

	var results []ScannableStruct
	query := `select * from (values ('one'), ('two'), ('three')) as _`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []ScannableStruct{{"one_scanned"}, {"two_scanned"}, {"three_scanned"}}
	eq(t, expected, results)
}

func TestQuery_struct_with_basic_types(t *testing.T) {
	ctx, conn := testInit(t)

	var result struct {
		Int32   int32           `db:"int32"`
		Int64   int64           `db:"int64"`
		Float32 float32         `db:"float32"`
		Float64 float64         `db:"float64"`
		String  string          `db:"string"`
		Bool    bool            `db:"bool"`
		Time    time.Time       `db:"time"`
		Scan    ScannableString `db:"scan"`
	}

	query := `
	select
		1                 :: int4      as int32,
		2                 :: int8      as int64,
		3                 :: float4    as float32,
		4                 :: float8    as float64,
		'5'               :: text      as string,
		true              :: bool      as bool,
		current_timestamp :: timestamp as time,
		'scan'            :: text      as scan
	`

	try(t, Query(ctx, conn, &result, query, nil))

	tFieldEq(t, "Int32", result.Int32, int32(1))
	tFieldEq(t, "Int64", result.Int64, int64(2))
	tFieldEq(t, "Float32", result.Float32, float32(3))
	tFieldEq(t, "Float64", result.Float64, float64(4))
	tFieldEq(t, "String", result.String, "5")
	tFieldEq(t, "Bool", result.Bool, true)
	if result.Time.IsZero() {
		t.Fatalf(`expected time to be non-zero`)
	}
	tFieldEq(t, "Scan", result.Scan, ScannableString("scan_scanned"))
}

func TestQuery_struct_field_naming(t *testing.T) {
	ctx, conn := testInit(t)

	var result struct {
		One   string   `db:"one"`
		Two   *string  `db:"six"`
		Three **string `db:"seven"`
		Four  string   `db:"-"`
		Five  string
	}

	two := "2"
	three := "3"
	three_ := &three

	result.One = "1"
	result.Two = &two
	result.Three = &three_
	result.Four = "4"
	result.Five = "5"

	query := `
	select
		'one'   as one,
		'two'   as six,
		'three' as seven
	`

	try(t, Query(ctx, conn, &result, query, nil))

	tFieldEq(t, "One", result.One, "one")
	tFieldEq(t, "Two", *result.Two, "two")
	tFieldEq(t, "Three", **result.Three, "three")
	tFieldEq(t, "Four", result.Four, "4")
	tFieldEq(t, "Five", result.Five, "5")
}

func TestQuery_struct_no_rows(t *testing.T) {
	ctx, conn := testInit(t)

	var result struct{}
	query := `select where false`
	err := Query(ctx, conn, &result, query, nil)
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf(`expected error ErrNoRows, got %+v`, err)
	}
}

func TestQuery_struct_multiple_rows(t *testing.T) {
	ctx, conn := testInit(t)

	var result struct {
		Val string `db:"val"`
	}
	query := `select * from (values ('one'), ('two')) as vals (val)`
	err := Query(ctx, conn, &result, query, nil)
	if !errors.Is(err, ErrMultipleRows) {
		t.Fatalf(`expected error ErrMultipleRows, got %+v`, err)
	}
}

func TestQuery_invalid_dest(t *testing.T) {
	ctx, conn := testInit(t)

	test := func(dest interface{}) {
		err := Query(ctx, conn, dest, `select true`, nil)
		if !errors.Is(err, ErrInvalidDest) {
			t.Fatalf(`expected error ErrInvalidDest, got %+v`, err)
		}
	}

	test(nil)
	test("str")
	test(struct{}{})
	test([]struct{}{})
}

func TestQuery_struct_field_nullability(t *testing.T) {
	ctx, conn := testInit(t)

	type Result struct {
		NonNilable string  `db:"non_nilable"`
		Nilable    *string `db:"nilable"`
	}

	var result Result

	query := `
	select
		'one' as non_nilable,
		null  as nilable
	`

	try(t, Query(ctx, conn, &result, query, nil))

	expected := Result{NonNilable: "one", Nilable: nil}
	if expected != result {
		t.Fatalf("expected %#v, got %#v", expected, result)
	}
}

func TestQuery_structs(t *testing.T) {
	ctx, conn := testInit(t)

	type Result struct {
		One string `db:"one"`
		Two int64  `db:"two"`
	}

	var results []Result
	query := `select * from (values ('one', 10), ('two', 20)) as vals (one, two)`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []Result{{"one", 10}, {"two", 20}}
	eq(t, expected, results)
}

func TestQuery_struct_missing_col_dest(t *testing.T) {
	ctx, conn := testInit(t)

	var result struct {
		One string `db:"one"`
	}

	{
		query := `select 'one' as one, 'two' as two`
		err := Query(ctx, conn, &result, query, nil)
		if !errors.Is(err, ErrNoColDest) {
			t.Fatalf(`expected error ErrNoColDest, got %+v`, err)
		}
	}

	{
		query := `select 'one' as one, null::text as two`
		err := Query(ctx, conn, &result, query, nil)
		if !errors.Is(err, ErrNoColDest) {
			t.Fatalf(`expected error ErrNoColDest, got %+v`, err)
		}
	}
}

func TestQuery_scalars_empty_result(t *testing.T) {
	ctx, conn := testInit(t)

	results := []string{"one", "two", "three"}
	query := `select where false`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []string{}
	eq(t, expected, results)
}

func TestQuery_structs_empty_result(t *testing.T) {
	ctx, conn := testInit(t)

	results := []struct{}{{}, {}, {}}
	query := `select where false`
	try(t, Query(ctx, conn, &results, query, nil))

	expected := []struct{}{}
	eq(t, expected, results)
}

func TestQuery_struct_nested_not_null_not_nilable(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val string `db:"val"`
	}
	type Nesting struct {
		Val    string `db:"val"`
		Nested Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		'two' as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: Nested{Val: "two"}}
	eq(t, expected, result)
}

func TestQuery_struct_nested_not_null_nilable_struct(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val string `db:"val"`
	}
	type Nesting struct {
		Val    string  `db:"val"`
		Nested *Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		'two' as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: &Nested{Val: "two"}}
	eq(t, expected, result)
}

func TestQuery_struct_nested_not_null_nilable_field(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val *string `db:"val"`
	}
	type Nesting struct {
		Val    string `db:"val"`
		Nested Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		'two' as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: Nested{Val: strPtr("two")}}
	eq(t, expected, result)
}

func TestQuery_struct_nested_not_null_nilable_both(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val *string `db:"val"`
	}
	type Nesting struct {
		Val    string  `db:"val"`
		Nested *Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		'two' as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: &Nested{Val: strPtr("two")}}
	eq(t, expected, result)
}

func TestQuery_struct_nested_null_not_nilable(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val string `db:"val"`
	}
	type Nesting struct {
		Val    string `db:"val"`
		Nested Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		null as "nested.val"
	`
	err := Query(ctx, conn, &result, query, nil)
	if !errors.Is(err, ErrNull) {
		t.Fatalf(`expected error ErrNull, got %+v`, err)
	}
}

// This also tests for on-demand allocation: if all fields of the inner struct
// are nil, the struct is not allocated.
func TestQuery_struct_nested_null_nilable_struct(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val string `db:"val"`
	}
	type Nesting struct {
		Val    string  `db:"val"`
		Nested *Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		null as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: nil}
	eq(t, expected, result)
}

func TestQuery_struct_nested_null_nilable_field(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val *string `db:"val"`
	}
	type Nesting struct {
		Val    string `db:"val"`
		Nested Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		null as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: Nested{Val: nil}}
	eq(t, expected, result)
}

// This also tests for on-demand allocation: if all fields of the inner struct
// are nil, the struct is not allocated.
func TestQuery_struct_nested_null_nilable_both(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		Val *string `db:"val"`
	}
	type Nesting struct {
		Val    string  `db:"val"`
		Nested *Nested `db:"nested"`
	}

	var result Nesting
	query := `
	select
		'one' as "val",
		null as "nested.val"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Val: "one", Nested: nil}
	eq(t, expected, result)
}

func TestQuery_struct_nested_partially_null(t *testing.T) {
	ctx, conn := testInit(t)

	type Nested struct {
		One *string `db:"one"`
		Two *string `db:"two"`
	}
	type Nesting struct {
		Nested *Nested `db:"nested"`
		Three  string  `db:"three"`
	}

	var result Nesting
	query := `
	select
		'one'   as "nested.one",
		'three' as "three"
	`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Nesting{Nested: &Nested{One: strPtr("one")}, Three: "three"}
	eq(t, expected, result)
}

/*
Fields without a matching source column must be left untouched. If they have
non-zero values, the existing values must be preserved.
*/
func TestQuery_struct_missing_col_src(t *testing.T) {
	ctx, conn := testInit(t)

	type Result struct {
		One   string  `db:"one"`
		Two   string  `db:"two"`
		Three *string `db:"-"`
		Four  *Result
	}

	result := Result{Two: "two", Three: strPtr("three"), Four: &Result{One: "four"}}
	query := `select 'one' as one`
	try(t, Query(ctx, conn, &result, query, nil))

	expected := Result{One: "one", Two: "two", Three: strPtr("three"), Four: &Result{One: "four"}}
	eq(t, expected, result)
}

func TestCols(t *testing.T) {
	type Nested struct {
		Val *string `db:"val"`
	}

	type Nesting struct {
		Val    string  `db:"val"`
		Nested *Nested `db:"nested"`
	}

	actual := sqlb.Cols(Nesting{})
	expected := `"val", ("nested")."val" as "nested.val"`
	if expected != actual {
		t.Fatalf(`expected Cols() to produce %q, got %q`, expected, actual)
	}
}

func createDb(connParams []string, dbName string) error {
	return withPostgresDb(connParams, func(db *sql.DB) error {
		_, err := db.Exec(`create database ` + dbName)
		return err
	})
}

func dropDb(connParams []string, dbName string) {
	err := withPostgresDb(connParams, func(db *sql.DB) error {
		_, err := db.Exec(`drop database if exists ` + dbName)
		return err
	})
	if err != nil {
		panic(err)
	}
}

func withPostgresDb(connParams []string, fun func(db *sql.DB) error) error {
	connParams = append(connParams, `dbname=postgres`)
	db, err := sql.Open("postgres", strings.Join(connParams, ` `))
	if err != nil {
		return err
	}
	err = fun(db)
	if err != nil {
		return err
	}
	return db.Close()
}

func testInit(t *testing.T) (context.Context, *sql.Tx) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	conn, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to start DB transaction: %+v", err)
	}

	return ctx, conn
}

func tFieldEq(t *testing.T, fieldName string, left interface{}, right interface{}) {
	if !reflect.DeepEqual(left, right) {
		t.Fatalf(`mismatch in field %q: %#v vs. %#v`, fieldName, left, right)
	}
}

type ScannableString string

func (self *ScannableString) Scan(input interface{}) error {
	*self = ScannableString(input.(string) + "_scanned")
	return nil
}

func strPtr(str string) *string { return &str }

func timeMustParse(str string) time.Time {
	out, err := time.Parse(time.RFC3339, str)
	if err != nil {
		panic(err)
	}
	return out
}

func try(t testing.TB, err error) {
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func eq(t testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		t.Fatalf("expected: %#v\nactual: %#v\n", exp, act)
	}
}

type ScannableStruct struct {
	Value string
}

func (self *ScannableStruct) Scan(input interface{}) error {
	switch input := input.(type) {
	case nil:
		return nil
	case string:
		self.Value = input + "_scanned"
		return nil
	case []byte:
		self.Value = string(input) + "_scanned"
		return nil
	default:
		return fmt.Errorf("unrecognized input for type %T: type %T, value %v", self, input, input)
	}
}
