package main

import (
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

var REALLY_BASIC = ReallyBasicTableDef()

func main() {
	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, REALLY_BASIC)

	result := "FAILED"

	ctx := cqlc.NewContext()

	err := ctx.Upsert(REALLY_BASIC).
		SetString(REALLY_BASIC.ID, "x").
		SetInt32(REALLY_BASIC.INT32_COLUMN, 222).
		Exec(session)

	if err != nil {
		log.Fatalf("Could not execute upsert: %v", err)
		os.Exit(1)
	}

	err = ctx.Delete(REALLY_BASIC.INT32_COLUMN).From(REALLY_BASIC).Where(REALLY_BASIC.ID.Eq("x")).Exec(session)

	if err != nil {
		log.Fatalf("Could not execute delete: %v", err)
		os.Exit(1)
	}

	basic := fetchFirstBasic(ctx, session, "x")

	if basic.Int32Column != 0 {
		log.Fatalf("Got bogus basic: %v", basic)
		os.Exit(1)
	}

	err = ctx.Delete().From(REALLY_BASIC).Where(REALLY_BASIC.ID.Eq("x")).Exec(session)

	if err != nil {
		log.Fatalf("Could not execute delete: %v", err)
		os.Exit(1)
	}

	basic = fetchFirstBasic(ctx, session, "x")

	if basic == nil {
		result = "PASSED"
	}

	os.Stdout.WriteString(result)
}

func fetchFirstBasic(ctx *cqlc.Context, s *gocql.Session, key string) *Basic {
	iter, err := ctx.Select().From(REALLY_BASIC).Where(REALLY_BASIC.ID.Eq("x")).Fetch(s)

	basics := BindBasic(iter)

	err = iter.Close()
	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	if len(basics) > 0 {
		return &basics[0]
	} else {
		return nil
	}

}
