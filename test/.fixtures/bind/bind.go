package main

import (
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"github.com/relops/gocql"
	"log"
	"os"
	"reflect"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, REALLY_BASIC)

	result := "FAILED"

	ctx := cqlc.NewContext()

	basic := ReallyBasic{
		Id:          "y",
		Int32Column: 2001,
	}

	err := ctx.Store(REALLY_BASIC.Bind(basic)).Exec(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	fetched := fetchFirstReallyBasic(ctx, session, "y")

	if reflect.DeepEqual(fetched, basic) {
		result = "PASSED"
	} else {
		result = fmt.Sprintf("[%+v] [%+v]", fetched, basic)
	}

	os.Stdout.WriteString(result)

}

func fetchFirstReallyBasic(ctx *cqlc.Context, s *gocql.Session, key string) ReallyBasic {
	iter, err := ctx.Select().From(REALLY_BASIC).Where(REALLY_BASIC.ID.Eq(key)).Fetch(s)

	basics := BindReallyBasic(iter)

	err = iter.Close()
	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	if len(basics) != 1 {
		log.Fatalf("Could not fetch data for key: %s", key)
		os.Exit(1)
	}

	return basics[0]

}
