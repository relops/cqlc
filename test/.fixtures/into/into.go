package main

import (
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"github.com/relops/gocql"
	"log"
	"math"
	"os"
	"reflect"
	"speter.net/go/exp/math/dec/inf"
	"time"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, BASIC)

	result := "FAILED"

	ctx := cqlc.NewContext()

	basic := Basic{
		Id:              "x",
		Int32Column:     999,
		Int64Column:     1 << 55,
		AsciiColumn:     "do-re-me",
		TimestampColumn: time.Date(1999, time.December, 31, 23, 59, 59, 59, time.UTC), // Keep it simple for reflect.DeepEqual
		BooleanColumn:   true,
		TextColumn:      "ipso",
		VarcharColumn:   "lorem",
		FloatColumn:     math.MaxFloat32,
		DoubleColumn:    math.MaxFloat64,
		DecimalColumn:   inf.NewDec(1, 9),
		TimeuuidColumn:  gocql.TimeUUID(),
		MapColumn:       map[string]string{"baz": "quux"},
		ArrayColumn:     []string{"baz", "quux"},
	}

	err := ctx.Store(BASIC.Bind(basic)).Exec(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	var int32Column int32
	var decimalColumn *inf.Dec

	err = ctx.Select().
		From(BASIC).
		Where(BASIC.ID.Eq("x")).
		Bind(BASIC.INT32_COLUMN.To(&int32Column), BASIC.DECIMAL_COLUMN.To(&decimalColumn)).
		FetchOne(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	if int32Column == 999 && reflect.DeepEqual(decimalColumn, basic.DecimalColumn) {
		result = "PASSED"
	} else {
		result = fmt.Sprintf("int32Column: %d, decimalColumn %v", int32Column, decimalColumn)
	}

	os.Stdout.WriteString(result)

}
