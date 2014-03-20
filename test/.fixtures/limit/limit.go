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
			SetInt64(EVENTS.SENSOR, int64(100)).
			SetTimeUUID(EVENTS.TIMESTAMP, gocql.TimeUUID()).
			SetFloat32(EVENTS.TEMPERATURE, 19.8).
			SetInt32(EVENTS.PRESSURE, 357).
			Batch(batch)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Fatalf("Could not execute batch: %v", err)
		os.Exit(1)
	}

	limit := 11

	iter, err := ctx.Select(EVENTS.SENSOR).
		From(EVENTS).
		Limit(limit).
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

	if count == limit {

		limit = 14
		//ctx.Debug = true
		iter, err = ctx.Select().
			From(EVENTS).
			Where(EVENTS.SENSOR.Eq(int64(100))).
			Limit(limit).
			Fetch(session)

		count := 0

		MapEvents(iter, func(e Events) (bool, error) {
			count++
			return true, nil
		})

		if count == limit {
			result = "PASSED"
		} else {
			result = fmt.Sprintf("Expected limit of %d; got %d rows", limit, count)
		}

	} else {
		result = fmt.Sprintf("Expected limit of %d; got %d rows", limit, count)
	}

	if err != nil {
		log.Fatalf("Could not execute query: %v", err)
		os.Exit(1)
	}

	os.Stdout.WriteString(result)
}
