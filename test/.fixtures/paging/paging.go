package main

import (
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
			SetInt64(EVENTS.SENSOR, int64(i)).
			SetTimeUUID(EVENTS.TIMESTAMP, gocql.TimeUUID()).
			SetFloat32(EVENTS.TEMPERATURE, 19.8).
			SetInt32(EVENTS.PRESSURE, 357).
			Batch(batch)
	}

	err := session.ExecuteBatch(batch)

	if err != nil {
		log.Fatalf("Could not execute batch: %v", err)
		os.Exit(1)
	}

	query, err := ctx.Select().From(EVENTS).Prepare(session)
	if err != nil {
		log.Fatalf("Could not prepare query: %v", err)
		os.Exit(1)
	}

	query.PageSize(10)
	iter := query.Iter()
	count := 0

	MapEvents(iter, func(e Events) (bool, error) {
		count++
		return true, nil
	})

	if err := iter.Close(); err != nil {
		log.Fatalf("Could not close iterator: %v", err)
		os.Exit(1)
	}

	if count == events {
		result = "PASSED"
	}

	os.Stdout.WriteString(result)
}
