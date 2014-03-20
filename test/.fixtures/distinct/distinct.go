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
	session.SetPageSize(1000)
	integration.Truncate(session, EVENTS)

	result := "FAILED"

	ctx := cqlc.NewContext()
	batch := gocql.NewBatch(gocql.LoggedBatch)

	rounds := 10
	distinct := 10

	for i := 0; i < rounds; i++ {
		for j := 0; j < distinct; j++ {
			ctx.Upsert(EVENTS).
				SetInt64(EVENTS.SENSOR, int64(j)).
				SetTimeUUID(EVENTS.TIMESTAMP, gocql.TimeUUID()).
				SetFloat32(EVENTS.TEMPERATURE, 19.8).
				SetInt32(EVENTS.PRESSURE, 357).
				Batch(batch)
		}
	}

	err := session.ExecuteBatch(batch)

	if err != nil {
		log.Fatalf("Could not execute batch: %v", err)
		os.Exit(1)
	}

	iter, err := ctx.SelectDistinct(EVENTS.SENSOR).From(EVENTS).Fetch(session)
	if err != nil {
		log.Fatalf("Could not prepare query: %v", err)
		os.Exit(1)
	}

	count := 0

	MapEvents(iter, func(e Events) (bool, error) {
		count++
		return true, nil
	})

	if err := iter.Close(); err != nil {
		log.Fatalf("Could not close iterator: %v", err)
		os.Exit(1)
	}

	if count == distinct {
		result = "PASSED"
	} else {
		result = fmt.Sprintf("Expected %d distinct rows; got %d", distinct, count)
	}

	os.Stdout.WriteString(result)
}
