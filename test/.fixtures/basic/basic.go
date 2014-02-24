package main

import (
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"github.com/relops/gocql"
	"log"
	"math"
	"math/big"
	"os"
	"reflect"
	"time"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, BASIC)

	result := "FAILED"

	ctx := cqlc.NewContext()

	basic := Basic{
		Id:              "x",
		Int32Column:     111,
		Int64Column:     1 << 32,
		AsciiColumn:     "ABC",
		TimestampColumn: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), // Keep it simple for reflect.DeepEqual
		BooleanColumn:   true,
		TextColumn:      "foo",
		VarcharColumn:   "bar",
		FloatColumn:     math.MaxFloat32,
		DoubleColumn:    math.MaxFloat64,
		DecimalColumn:   big.NewRat(1, 3),
		TimeuuidColumn:  gocql.TimeUUID(),
		MapColumn:       map[string]string{"baz": "quux"},
		ArrayColumn:     []string{"baz", "quux"},
	}

	create(ctx, session, basic)

	iter, err := ctx.Select(
		BASIC.ID,
		BASIC.ASCII_COLUMN,
		BASIC.INT32_COLUMN,
		BASIC.INT64_COLUMN,
		BASIC.FLOAT_COLUMN,
		BASIC.DOUBLE_COLUMN,
		BASIC.DECIMAL_COLUMN,
		BASIC.TIMESTAMP_COLUMN,
		BASIC.TIMEUUID_COLUMN,
		BASIC.BOOLEAN_COLUMN,
		BASIC.TEXT_COLUMN,
		BASIC.VARCHAR_COLUMN,
		BASIC.MAP_COLUMN,
		BASIC.ARRAY_COLUMN).
		From(BASIC).
		Where(BASIC.ID.Eq("x")).
		Fetch(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	result, _ = checkBasics(iter, basic)

	iter, err = ctx.Select(
		BASIC.ID,
		BASIC.ASCII_COLUMN,
		BASIC.INT32_COLUMN,
		BASIC.INT64_COLUMN,
		BASIC.FLOAT_COLUMN,
		BASIC.DOUBLE_COLUMN,
		BASIC.DECIMAL_COLUMN,
		BASIC.TIMESTAMP_COLUMN,
		BASIC.TIMEUUID_COLUMN,
		BASIC.BOOLEAN_COLUMN,
		BASIC.TEXT_COLUMN,
		BASIC.VARCHAR_COLUMN,
		BASIC.MAP_COLUMN,
		BASIC.ARRAY_COLUMN).
		From(BASIC).
		Fetch(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	result, _ = checkBasics(iter, basic)

	// TODO write test case for a non-matching WHERE clause

	os.Stdout.WriteString(result)
}

func checkBasics(iter *gocql.Iter, basic Basic) (string, error) {
	result := "FAILED"
	basics := BindBasic(iter)

	err := iter.Close()
	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		return "", err
	}

	if len(basics) == 1 {
		if reflect.DeepEqual(basics[0], basic) {
			result = "PASSED"
		} else {
			result = fmt.Sprintf("[%+v] [%+v]", basics[0], basic)
		}
	}
	return result, err
}

func create(ctx *cqlc.Context, s *gocql.Session, basic Basic) {

	err := ctx.Upsert(BASIC).
		SetString(BASIC.ID, basic.Id).
		SetInt32(BASIC.INT32_COLUMN, basic.Int32Column).
		SetInt64(BASIC.INT64_COLUMN, basic.Int64Column).
		SetFloat32(BASIC.FLOAT_COLUMN, basic.FloatColumn).
		SetFloat64(BASIC.DOUBLE_COLUMN, basic.DoubleColumn).
		SetString(BASIC.ASCII_COLUMN, basic.AsciiColumn).
		SetTimestamp(BASIC.TIMESTAMP_COLUMN, basic.TimestampColumn).
		SetTimeUUID(BASIC.TIMEUUID_COLUMN, basic.TimeuuidColumn).
		SetBoolean(BASIC.BOOLEAN_COLUMN, basic.BooleanColumn).
		SetString(BASIC.TEXT_COLUMN, basic.TextColumn).
		SetString(BASIC.VARCHAR_COLUMN, basic.VarcharColumn).
		SetMap(BASIC.MAP_COLUMN, basic.MapColumn).
		SetArray(BASIC.ARRAY_COLUMN, basic.ArrayColumn).
		SetDecimal(BASIC.DECIMAL_COLUMN, basic.DecimalColumn).
		Exec(s)

	if err != nil {
		log.Fatalf("Could not execute query: %v", err)
		os.Exit(1)
	}
}
