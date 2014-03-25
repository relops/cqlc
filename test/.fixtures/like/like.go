package main

import (
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
	"time"
)

type OriginalLike interface {
	CommentValue() string
	DateValue() time.Time
	IdValue() string
}

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, ORIGINAL)

	result := "FAILED"

	ctx := cqlc.NewContext()

	o := Original{
		Id:      "x",
		Date:    time.Now().UTC().Truncate(time.Millisecond),
		Comment: "foo",
	}

	if err := ctx.Store(ORIGINAL.Bind(o)).Exec(session); err != nil {
		log.Fatalf("Could not upsert ORIGINAL: %v", err)
		os.Exit(1)
	}

	iter, err := ctx.Select().From(ORIGINAL).Fetch(session)

	if err != nil {
		log.Fatalf("Could not read ORIGINAL: %v", err)
		os.Exit(1)
	}

	var c Clone

	err = MapOriginal(iter, func(o Original) (bool, error) {
		c = duplicate(&o)
		return false, nil
	})

	if err != nil {
		log.Fatalf("Could not map ORIGINAL: %v", err)
		os.Exit(1)
	}

	if o.Id == c.Id && o.Date == c.Date {
		result = "PASSED"
	} else {
		result = fmt.Sprintf("Original: %+v; clone: %+v", o, c)
	}

	os.Stdout.WriteString(result)
}

func duplicate(o OriginalLike) Clone {
	return Clone{
		Id:      o.IdValue(),
		Date:    o.DateValue(),
		Comment: o.CommentValue(),
	}
}
