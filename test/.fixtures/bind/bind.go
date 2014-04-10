package main

import (
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
	"reflect"
)

func main() {

	s := integration.TestSession("127.0.0.1", "cqlc")
	cqlc.Truncate(s, REALLY_BASIC)

	result := "FAILED"

	ctx := cqlc.NewContext()

	basic := ReallyBasic{
		Id:          "y",
		Int32Column: 2001,
	}

	err := ctx.Store(REALLY_BASIC.Bind(basic)).Exec(s)

	if err != nil {
		log.Fatalf("Could not store data: %v", err)
		os.Exit(1)
	}

	query := ctx.Select().From(REALLY_BASIC).Where(REALLY_BASIC.ID.Eq("y"))

	var fetched ReallyBasic
	found, err := query.Into(REALLY_BASIC.To(&fetched)).FetchOne(s)

	if err != nil {
		log.Fatalf("Could not retrieve data: %v", err)
		os.Exit(1)
	}

	if found {
		if reflect.DeepEqual(fetched, basic) {
			result = "PASSED"
		} else {
			result = fmt.Sprintf("[%+v] [%+v]", fetched, basic)
		}
	}

	os.Stdout.WriteString(result)
}
