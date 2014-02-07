package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

var COUNTER = BasicCounterTableDef()

func main() {
	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, COUNTER)

	result := "FAILED"

	ctx := cqlc.NewContext()

	err := ctx.UpdateCounter(COUNTER).
		Increment(COUNTER.COUNTER_COLUMN, 13).
		Having(COUNTER.ID.Eq("x")).
		Exec(session)

	if err != nil {
		log.Fatalf("Could not execute counter increment: %v", err)
	}

	counter := readCounter(session, "x")
	if counter == 13 {
		result = "PASSED"
	}

	c := BasicCounter{
		Id:            "x",
		CounterColumn: 11,
	}

	err = ctx.Add(COUNTER.Bind(c)).Exec(session)

	if err != nil {
		log.Fatalf("Could not execute counter increment: %v", err)
	}

	counter = readCounter(session, "x")
	if counter != 24 {
		result = fmt.Sprintf("Expected 24, but counter was %d", counter)
	}

	os.Stdout.WriteString(result)
}

func readCounter(session *gocql.Session, key string) int64 {

	var counter int64

	ctx := cqlc.NewContext()
	err := ctx.Select(COUNTER.COUNTER_COLUMN).
		From(COUNTER).
		Where(COUNTER.ID.Eq(key)).
		Bind(COUNTER.COUNTER_COLUMN.To(&counter)).
		FetchOne(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	return counter
}
