package main

import (
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

var FIRST_TIMELINE = FirstTimelineTableDef()
var SECOND_TIMELINE = SecondTimelineTableDef()

type WhenRowKey interface {
	cqlc.Table
	WhenColumn() cqlc.LastPartitionedTimeUUIDColumn
}

func main() {
	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, FIRST_TIMELINE)
	integration.Truncate(session, SECOND_TIMELINE)

	result := "FAILED"

	timestamp := gocql.TimeUUID()

	first := FirstTimeline{
		When: timestamp,
		Tag:  "foobar",
	}

	second := SecondTimeline{
		When:      timestamp,
		Latitude:  50.12,
		Longitude: 0.87,
	}

	ctx := cqlc.NewContext()
	err := ctx.Store(FIRST_TIMELINE.Bind(first)).Exec(session)
	err = ctx.Store(SECOND_TIMELINE.Bind(second)).Exec(session)

	if err != nil {
		log.Fatalf("Could not execute upsert: %v", err)
		os.Exit(1)
	}

	err = deleteByTimestamp(session, FIRST_TIMELINE, timestamp)
	err = deleteByTimestamp(session, SECOND_TIMELINE, timestamp)

	if err != nil {
		log.Fatalf("Could not execute delete: %v", err)
		os.Exit(1)
	}

	var tag string
	var latitude float32

	err = ctx.Select().
		From(FIRST_TIMELINE).
		Where(FIRST_TIMELINE.WHEN.Eq(timestamp)).
		Bind(FIRST_TIMELINE.TAG.To(&tag)).
		FetchOne(session)

	err = ctx.Select().
		From(SECOND_TIMELINE).
		Where(SECOND_TIMELINE.WHEN.Eq(timestamp)).
		Bind(SECOND_TIMELINE.LATITUDE.To(&latitude)).
		FetchOne(session)

	if err != nil {
		log.Fatalf("Could not execute select: %v", err)
		os.Exit(1)
	}

	if tag == "" && latitude == 0.0 {
		result = "PASSED"
	}

	os.Stdout.WriteString(result)
}

func deleteByTimestamp(s *gocql.Session, w WhenRowKey, t gocql.UUID) error {
	ctx := cqlc.NewContext()
	return ctx.Delete().From(w).Where(w.WhenColumn().Eq(t)).Exec(s)
}
