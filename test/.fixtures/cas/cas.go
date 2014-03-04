package main

import (
	//"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, REALLY_BASIC)

	result := "FAILED"

	applied, _, _ := compareAndSwap(session, "a", 1)

	if applied {

		applied, id, int32column := compareAndSwap(session, "a", 1)

		if !applied {
			if id == "a" && int32column == 1 {
				result = "PASSED"
			}
		}

		// TODO Right now there is no support for CAS operations
		// on UPDATE statements so because this lacks a WHERE
		// clause, the value cannot be upated for a key that
		// already exists

		applied, id, int32column = compareAndSwap(session, "a", 2)

		if !applied {
			if id == "a" && int32column == 1 {
				result = "PASSED"
			}
		}
	}

	os.Stdout.WriteString(result)

}

func compareAndSwap(session *gocql.Session, id string, int32column int32) (bool, string, int32) {

	var casId string
	var casInt32column int32

	ctx := cqlc.NewContext()

	applied, err := ctx.Upsert(REALLY_BASIC).
		SetString(REALLY_BASIC.ID, id).
		SetInt32(REALLY_BASIC.INT32_COLUMN, int32column).
		IfExists(REALLY_BASIC.ID.To(&casId), REALLY_BASIC.INT32_COLUMN.To(&casInt32column)).
		Swap(session)

	if err != nil {
		log.Fatalf("Could not execute CAS statement: %v", err)
		os.Exit(1)
	}

	return applied, casId, casInt32column
}
