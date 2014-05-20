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

	s := integration.ClusterTestSession("127.0.0.1")
	result := runWithKeyspace(s, "cqlc")
	if result == "PASSED" {
		result = runWithKeyspace(s, "cqlc2")
	}
	os.Stdout.WriteString(result)
}

func runWithKeyspace(s *gocql.Session, keyspace string) string {

	truncate := fmt.Sprintf("TRUNCATE %s.shared", keyspace)
	if err := s.Query(truncate).Exec(); err != nil {
		log.Fatalf("Could not connect to cassandra: %v", err)
		os.Exit(1)
	}

	result := "FAILED"

	ctx := cqlc.NewContext()
	ctx.Keyspace = keyspace

	shared := Shared{
		Id:    "foo",
		Value: "bar",
	}

	err := ctx.Store(SHARED.Bind(shared)).Exec(s)

	if err != nil {
		log.Fatalf("Could not store data: %v", err)
		os.Exit(1)
	}

	found, value := get(s, ctx, "foo")

	if found && value == "bar" {

		err := ctx.Upsert(SHARED).SetString(SHARED.VALUE, "baz").Where(SHARED.ID.Eq("foo")).Exec(s)
		if err != nil {
			log.Fatalf("Could not upsert row: %v", err)
			os.Exit(1)
		}

		found, value := get(s, ctx, "foo")

		if found && value == "baz" {

			err = ctx.Delete().From(SHARED).Where(SHARED.ID.Eq("foo")).Exec(s)
			if err != nil {
				log.Fatalf("Could not delete row: %v", err)
				os.Exit(1)
			}

			found, _ := get(s, ctx, "foo")

			if !found {
				result = "PASSED"
			}
		}

	}

	return result
}

func get(s *gocql.Session, ctx *cqlc.Context, key string) (bool, string) {
	var value string

	found, err := ctx.Select(SHARED.VALUE).
		From(SHARED).
		Where(SHARED.ID.Eq("foo")).
		Bind(SHARED.VALUE.To(&value)).
		FetchOne(s)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}
	return found, value
}
