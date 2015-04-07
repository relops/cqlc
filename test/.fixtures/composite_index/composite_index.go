package main

import (
	//"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

func main() {
	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, SIMPLE_INDEXED_COMPOSITE)

	result := "FAILED"

	ctx := cqlc.NewContext()

	s := SimpleIndexedComposite{
		X: 1,
		Y: 2,
		Z: 3,
	}

	if err := ctx.Store(SIMPLE_INDEXED_COMPOSITE.Bind(s)).Exec(session); err != nil {
		log.Fatalf("Could not upsert ORIGINAL: %v", err)
		os.Exit(1)
	}

	var z int32

	found, err := ctx.Select(SIMPLE_INDEXED_COMPOSITE.Z).
		From(SIMPLE_INDEXED_COMPOSITE).
		Where(SIMPLE_INDEXED_COMPOSITE.X.Eq(s.X), SIMPLE_INDEXED_COMPOSITE.Y.Eq(s.Y)).
		Bind(SIMPLE_INDEXED_COMPOSITE.Z.To(&z)).
		FetchOne(session)

	if !found {
		log.Fatalf("Could not find SIMPLE_INDEXED_COMPOSITE with key %v", 1)
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Could not retrieve SIMPLE_INDEXED_COMPOSITE: %v", err)
		os.Exit(1)
	}

	if z == s.Z {
		result = "PASSED"
	}

	os.Stdout.WriteString(result)
}
