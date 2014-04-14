package main

import (
	"fmt"
	//"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	//"math"
	"os"
	"reflect"
	//"speter.net/go/exp/math/dec/inf"
	//"time"
)

func main() {

	s := integration.TestSession("127.0.0.1", "cqlc")
	cqlc.Truncate(s, COLLECTIONS)

	result := "FAILED"

	input := Collections{
		Id: 10,
	}

	input.StringColumn = []string{"a", "b", "c"}

	ctx := cqlc.NewContext()
	if err := ctx.Store(COLLECTIONS.Bind(input)).Exec(s); err != nil {
		log.Fatalf("Could not store collections: %v", err)
		os.Exit(1)
	}

	var output Collections
	found, err := ctx.Select().From(COLLECTIONS).Where(COLLECTIONS.ID.Eq(10)).Into(COLLECTIONS.To(&output)).FetchOne(s)
	if err != nil {
		log.Fatalf("Could not store collections: %v", err)
		os.Exit(1)
	}

	if found {
		if reflect.DeepEqual(input, output) {
			result = "PASSED"
		} else {
			result = fmt.Sprintf("Expected %+v but got %+v", input, output)
		}
	}

	os.Stdout.WriteString(result)
}
