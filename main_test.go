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
)

var testCtx = context.Background()
var testTx *sql.Tx

const testDbName = `gos_test_db`

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
	names to allow multiple instances of the test, but that seems like decadent
	overkill.
	*/
	_ = dropDb(connParams, testDbName)
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

	/**
	Use a single DB transaction for testing, rolling it back at the end, to avoid
	storing test-generated garbage even if database deletion fails.
	*/
	testTx, err = db.Begin()
	if err != nil {
		panic(fmt.Errorf("failed to start DB transaction: %w", err))
	}
	defer testTx.Rollback()

	return m.Run()
}

func TestScalarBasic(t *testing.T) {
	var result string
	query := `select 'blah'`
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := "blah"
	if expected != result {
		t.Fatalf(`expected %q, got %q`, expected, result)
	}
}

func TestScalarNonNullable(t *testing.T) {
	var result string
	query := `select null`
	err := Query(testCtx, testTx, &result, query, nil)
	/**
	Why this doesn't inspect the error: the error comes from `database/sql`;
	there's no programmatic API to detect its type. We return an `ErrNull` in
	some other scenarios.
	*/
	if err == nil {
		t.Fatalf(`expected scanning null into non-nullable scalar to produce an error`)
	}
}

func TestScalarNullable(t *testing.T) {
	var result *string
	query := `select 'blah'`
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := "blah"
	if expected != *result {
		t.Fatalf(`expected %q, got %q`, expected, *result)
	}

	query = `select null`
	err = Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if result != nil {
		t.Fatalf(`expected selecting null to produce nil, got %q`, *result)
	}
}

func TestScalarsBasic(t *testing.T) {
	var results []string
	query := `select * from (values ('one'), ('two'), ('three')) as _`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := []string{"one", "two", "three"}
	if !reflect.DeepEqual(expected, results) {
		t.Fatalf(`expected %#v, got %#v`, expected, results)
	}
}

func TestScalarsNonNullable(t *testing.T) {
	var results []string
	query := `select * from (values ('one'), (null), ('three')) as _`
	err := Query(testCtx, testTx, &results, query, nil)
	/**
	Why this doesn't inspect the error: the error comes from `database/sql`;
	there's no programmatic API to detect its type. We return an `ErrNull` in
	some other scenarios.
	*/
	if err == nil {
		t.Fatalf(`expected scanning null into non-nullable scalar to produce an error`)
	}
}

func TestScalarsNullable(t *testing.T) {
	var results []*string
	query := `select * from (values ('one'), (null), ('three')) as _`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := []*string{strptr("one"), nil, strptr("three")}
	if !reflect.DeepEqual(expected, results) {
		t.Fatalf(`expected %#v, got %#v`, expected, results)
	}
}

// Verify that we treat `time.Time` as an atomic scannable rather than a struct.
func TestScalarTime(t *testing.T) {
	var result time.Time
	query := `select '0001-01-01'::timestamp`
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := timeMustParse(`0001-01-01T00:00:00Z`)
	if expected.UnixNano() != result.UnixNano() {
		t.Fatalf(`expected %v, got %v`, expected, result)
	}
}

// Verify that we treat `[]time.Time` as atomic scannables rather than structs.
func TestScalarsTime(t *testing.T) {
	var results []time.Time
	query := `select * from (values ('0001-01-01'::timestamp), ('0002-01-01'::timestamp)) as _`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := []int64{
		timeMustParse(`0001-01-01T00:00:00Z`).UnixNano(),
		timeMustParse(`0002-01-01T00:00:00Z`).UnixNano(),
	}

	received := []int64{
		results[0].UnixNano(),
		results[1].UnixNano(),
	}

	if !reflect.DeepEqual(expected, received) {
		t.Fatalf(`expected %#v, got %#v`, expected, received)
	}
}

func TestScalarScannable(t *testing.T) {
	var result ScannableString
	query := `select 'blah'`
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := "blah_scanned"
	received := string(result)
	if expected != received {
		t.Fatalf(`expected %q, got %q`, expected, received)
	}
}

func TestScalarsScannable(t *testing.T) {
	var results []ScannableString
	query := `select * from (values ('one'), ('two'), ('three')) as _`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := []string{"one_scanned", "two_scanned", "three_scanned"}
	received := *(*[]string)(unsafe.Pointer(&results))
	if !reflect.DeepEqual(expected, received) {
		t.Fatalf(`expected %#v, got %#v`, expected, received)
	}
}

func TestStructScannable(t *testing.T) {
	var result ScannableStruct
	query := `select 'blah'`
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := ScannableStruct{Value: "blah_scanned"}
	if expected != result {
		t.Fatalf(`expected %q, got %q`, expected, result)
	}
}

func TestStructsScannable(t *testing.T) {
	var results []ScannableStruct
	query := `select * from (values ('one'), ('two'), ('three')) as _`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := []ScannableStruct{{"one_scanned"}, {"two_scanned"}, {"three_scanned"}}
	if !reflect.DeepEqual(expected, results) {
		t.Fatalf(`expected %#v, got %#v`, expected, results)
	}
}

func TestStructWithBasicTypes(t *testing.T) {
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

	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

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

func TestStructFieldNaming(t *testing.T) {
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

	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	tFieldEq(t, "One", result.One, "one")
	tFieldEq(t, "Two", *result.Two, "two")
	tFieldEq(t, "Three", **result.Three, "three")
	tFieldEq(t, "Four", result.Four, "4")
	tFieldEq(t, "Five", result.Five, "5")
}

func TestStructNoRows(t *testing.T) {
	var result struct{}
	query := `select where false`
	err := Query(testCtx, testTx, &result, query, nil)
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf(`expected error ErrNoRows, got %+v`, err)
	}
}

func TestStructMultipleRows(t *testing.T) {
	var result struct {
		Val string `db:"val"`
	}
	query := `select * from (values ('one'), ('two')) as vals (val)`
	err := Query(testCtx, testTx, &result, query, nil)
	if !errors.Is(err, ErrMultipleRows) {
		t.Fatalf(`expected error ErrMultipleRows, got %+v`, err)
	}
}

func TestInvalidDest(t *testing.T) {
	err := Query(testCtx, testTx, nil, `select`, nil)
	if !errors.Is(err, ErrInvalidDest) {
		t.Fatalf(`expected error ErrInvalidDest, got %+v`, err)
	}
	err = Query(testCtx, testTx, "str", `select`, nil)
	if !errors.Is(err, ErrInvalidDest) {
		t.Fatalf(`expected error ErrInvalidDest, got %+v`, err)
	}
	err = Query(testCtx, testTx, struct{}{}, `select`, nil)
	if !errors.Is(err, ErrInvalidDest) {
		t.Fatalf(`expected error ErrInvalidDest, got %+v`, err)
	}
	err = Query(testCtx, testTx, []struct{}{}, `select`, nil)
	if !errors.Is(err, ErrInvalidDest) {
		t.Fatalf(`expected error ErrInvalidDest, got %+v`, err)
	}
}

func TestStructFieldNullability(t *testing.T) {
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

	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Result{NonNilable: "one", Nilable: nil}
	if expected != result {
		t.Fatalf("expected %#v, got %#v", expected, result)
	}
}

func TestStructs(t *testing.T) {
	type Result struct {
		One string `db:"one"`
		Two int64  `db:"two"`
	}

	var results []Result
	query := `select * from (values ('one', 10), ('two', 20)) as vals (one, two)`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := []Result{{"one", 10}, {"two", 20}}
	if !reflect.DeepEqual(expected, results) {
		t.Fatalf(`expected %#v, got %#v`, expected, results)
	}
}

func TestStructMissingColDest(t *testing.T) {
	var result struct {
		One string `db:"one"`
	}

	{
		query := `select 'one' as one, 'two' as two`
		err := Query(testCtx, testTx, &result, query, nil)
		if !errors.Is(err, ErrNoColDest) {
			t.Fatalf(`expected error ErrNoColDest, got %+v`, err)
		}
	}

	{
		query := `select 'one' as one, null as two`
		err := Query(testCtx, testTx, &result, query, nil)
		if !errors.Is(err, ErrNoColDest) {
			t.Fatalf(`expected error ErrNoColDest, got %+v`, err)
		}
	}
}

func TestScalarsEmptyResult(t *testing.T) {
	results := []string{"one", "two", "three"}
	query := `select where false`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	expected := []string{}
	if !reflect.DeepEqual(expected, results) {
		t.Fatalf(`expected %#v, got %#v`, expected, results)
	}
}

func TestStructsEmptyResult(t *testing.T) {
	results := []struct{}{{}, {}, {}}
	query := `select where false`
	err := Query(testCtx, testTx, &results, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	expected := []struct{}{}
	if !reflect.DeepEqual(expected, results) {
		t.Fatalf(`expected %#v, got %#v`, expected, results)
	}
}

func TestStructNestedNotNullNotNilable(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: Nested{Val: "two"}}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestStructNestedNotNullNilableStruct(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: &Nested{Val: "two"}}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestStructNestedNotNullNilableField(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: Nested{Val: strptr("two")}}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestStructNestedNotNullNilableBoth(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: &Nested{Val: strptr("two")}}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestStructNestedNullNotNilable(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if !errors.Is(err, ErrNull) {
		t.Fatalf(`expected error ErrNull, got %+v`, err)
	}
}

// This also tests for on-demand allocation: if all fields of the inner struct
// are nil, the struct is not allocated.
func TestStructNestedNullNilableStruct(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: nil}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestStructNestedNullNilableField(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: Nested{Val: nil}}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

// This also tests for on-demand allocation: if all fields of the inner struct
// are nil, the struct is not allocated.
func TestStructNestedNullNilableBoth(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Val: "one", Nested: nil}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestStructNestedPartiallyNull(t *testing.T) {
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
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Nesting{Nested: &Nested{One: strptr("one")}, Three: "three"}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

/*
Fields without a matching source column must be left untouched. If they have
non-zero values, the existing values must be preserved.
*/
func TestStructMissingColSrc(t *testing.T) {
	type Result struct {
		One   string  `db:"one"`
		Two   string  `db:"two"`
		Three *string `db:"-"`
		Four  *Result
	}

	result := Result{Two: "two", Three: strptr("three"), Four: &Result{One: "four"}}
	query := `select 'one' as one`
	err := Query(testCtx, testTx, &result, query, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Result{One: "one", Two: "two", Three: strptr("three"), Four: &Result{One: "four"}}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf(`expected %#v, got %#v`, expected, result)
	}
}

func TestCols(t *testing.T) {
	type Nested struct {
		Val *string `db:"val"`
	}

	type Nesting struct {
		Val    string  `db:"val"`
		Nested *Nested `db:"nested"`
	}

	actual := Cols(Nesting{})
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

func dropDb(connParams []string, dbName string) error {
	return withPostgresDb(connParams, func(db *sql.DB) error {
		_, err := db.Exec(`drop database if exists ` + dbName)
		return err
	})
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

func tFieldEq(t *testing.T, fieldName string, left interface{}, right interface{}) {
	if !reflect.DeepEqual(left, right) {
		t.Fatalf(`mismatch in field %q: %#v vs. %#v`, fieldName, left, right)
	}
}

func tExec(t *testing.T, query string) {
	_, err := testTx.Exec(query)
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

type ScannableString string

func (self *ScannableString) Scan(input interface{}) error {
	*self = ScannableString(input.(string) + "_scanned")
	return nil
}

func strptr(str string) *string { return &str }

func timeMustParse(str string) time.Time {
	out, err := time.Parse(time.RFC3339, str)
	if err != nil {
		panic(err)
	}
	return out
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
