package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, EVENTS)

	result := "FAILED"

	ctx := cqlc.NewContext()
	batch := gocql.NewBatch(gocql.LoggedBatch)

	events := 100

	for i := 0; i < events; i++ {
		ctx.Upsert(EVENTS).
			SetInt64(EVENTS.SENSOR, 100).
			SetTimeUUID(EVENTS.TIMESTAMP, gocql.TimeUUID()).
			SetFloat32(EVENTS.TEMPERATURE, 19.8).
			SetInt32(EVENTS.PRESSURE, 357).
			Batch(batch)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Fatalf("Could not execute batch: %v", err)
		os.Exit(1)
	}

	iter, err := ctx.Select().
		From(EVENTS).
		Where(EVENTS.SENSOR.Eq(100)).
		OrderBy(EVENTS.TIMESTAMP).
		Fetch(session)

	if err != nil {
		log.Fatalf("Could not execute query: %v", err)
		os.Exit(1)
	}

	count := 0

	MapEvents(iter, func(e Events) (bool, error) {
		count++
		return true, nil
	})

	if count == events {
		result = "PASSED"
	} else {
		result = fmt.Sprintf("Expected %d rows; got %d rows", events, count)
	}

	os.Stdout.WriteString(result)
}
