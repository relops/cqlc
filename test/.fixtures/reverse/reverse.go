package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
	"time"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, REVERSE_TIMESERIES)

	result := "FAILED"

	ctx := cqlc.NewContext()
	batch := gocql.NewBatch(gocql.LoggedBatch)

	events := 100

	for i := 0; i < events; i++ {

		unix := time.Now().Unix()
		t := time.Unix(unix+int64(i), 0)

		ctx.Upsert(REVERSE_TIMESERIES).
			SetString(REVERSE_TIMESERIES.EVENT_TYPE, "x").
			SetTimestamp(REVERSE_TIMESERIES.INSERTION_TIME, t).
			SetBytes(REVERSE_TIMESERIES.EVENT, []byte("neb")).
			Batch(batch)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Fatalf("Could not execute batch: %v", err)
		os.Exit(1)
	}

	iter, err := ctx.Select().From(REVERSE_TIMESERIES).Fetch(session)
	if err != nil {
		log.Fatalf("Could not read REVERSE_TIMESERIES: %v", err)
		os.Exit(1)
	}

	var previous time.Time

	err = MapReverseTimeseries(iter, func(e ReverseTimeseries) (bool, error) {

		current := e.InsertionTime

		if !previous.IsZero() {
			if current.After(previous) {
				return false, fmt.Errorf("Wrong ordering (DESC): previous was %v but current is %v", previous, current)
			}
		}

		previous = current
		return true, nil
	})

	if err != nil {
		log.Fatalf("Unexpected order of REVERSE_TIMESERIES: %v", err)
		os.Exit(1)
	}

	result = "PASSED"

	os.Stdout.WriteString(result)
}
