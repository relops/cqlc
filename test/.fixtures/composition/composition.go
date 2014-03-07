package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"math"
	"os"
)

type WhenRowKey interface {
	cqlc.Table
	WhenColumn() cqlc.LastPartitionedTimeUUIDColumn
	SupportsUpsert() bool
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

	var tag string
	var latitude float32

	f1, err := fetchOne(ctx, session, FIRST_TIMELINE, timestamp, FIRST_TIMELINE.TAG.To(&tag))
	f2, err := fetchOne(ctx, session, SECOND_TIMELINE, timestamp, SECOND_TIMELINE.LATITUDE.To(&latitude))

	if err != nil {
		log.Fatalf("Could not execute select: %v", err)
		os.Exit(1)
	}

	if f1 && f2 && tag == "foobar" && math.Float32bits(latitude) == math.Float32bits(50.12) {

		// TODO Implement a FROM binding
		t := "bar"
		l := float32(72.34)

		err = upsert(ctx, session, FIRST_TIMELINE, timestamp, FIRST_TIMELINE.TAG.To(&t))
		err = upsert(ctx, session, SECOND_TIMELINE, timestamp, SECOND_TIMELINE.LATITUDE.To(&l))

		if err != nil {
			log.Fatalf("Could not execute upsert: %v", err)
			os.Exit(1)
		}

		f1, err = fetchOne(ctx, session, FIRST_TIMELINE, timestamp, FIRST_TIMELINE.TAG.To(&tag))
		f2, err = fetchOne(ctx, session, SECOND_TIMELINE, timestamp, SECOND_TIMELINE.LATITUDE.To(&latitude))

		if err != nil {
			log.Fatalf("Could not execute select: %v", err)
			os.Exit(1)
		}

		if f1 && f2 && tag == t && math.Float32bits(latitude) == math.Float32bits(l) {

			err = deleteByTimestamp(ctx, session, FIRST_TIMELINE, timestamp)
			err = deleteByTimestamp(ctx, session, SECOND_TIMELINE, timestamp)

			if err != nil {
				log.Fatalf("Could not execute delete: %v", err)
				os.Exit(1)
			}

			var tag string
			var latitude float32

			f1, err = fetchOne(ctx, session, FIRST_TIMELINE, timestamp, FIRST_TIMELINE.TAG.To(&tag))
			f2, err = fetchOne(ctx, session, SECOND_TIMELINE, timestamp, SECOND_TIMELINE.LATITUDE.To(&latitude))

			if err != nil {
				log.Fatalf("Could not execute select: %v", err)
				os.Exit(1)
			}

			if !f1 && !f2 && tag == "" && latitude == 0.0 {
				result = "PASSED"
			} else {
				result = fmt.Sprintf("After delete - Tag was: %s; Latitude was %f", tag, latitude)
			}
		} else {
			result = fmt.Sprintf("After upsert - Tag was: %s; Latitude was %f", tag, latitude)
		}

	} else {
		result = fmt.Sprintf("Before delete - Tag was: %s; Latitude was %f", tag, latitude)
	}

	os.Stdout.WriteString(result)
}

func upsert(ctx *cqlc.Context, s *gocql.Session, w WhenRowKey, t gocql.UUID, binding cqlc.ColumnBinding) error {
	return ctx.Upsert(w).
		Apply(binding).
		Where(w.WhenColumn().Eq(t)).
		Exec(s)
}

func fetchOne(ctx *cqlc.Context, s *gocql.Session, w WhenRowKey, t gocql.UUID, binding cqlc.ColumnBinding) (bool, error) {
	return ctx.
		Select().
		From(w).
		Where(w.WhenColumn().Eq(t)).
		Bind(binding).
		FetchOne(s)
}

func deleteByTimestamp(ctx *cqlc.Context, s *gocql.Session, w WhenRowKey, t gocql.UUID) error {
	return ctx.
		Delete().
		From(w).
		Where(w.WhenColumn().Eq(t)).
		Exec(s)
}
