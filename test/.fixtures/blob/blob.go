package main

import (
	"bytes"
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, BASIC_BLOB)

	result := "FAILED"

	ctx := cqlc.NewContext()

	blob := BasicBlob{
		Id:         "baz",
		BlobColumn: []byte("foo"),
	}

	err := ctx.Store(BASIC_BLOB.Bind(blob)).Exec(session)

	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	var b []byte

	found, err := ctx.Select().
		From(BASIC_BLOB).
		Where(BASIC_BLOB.ID.Eq("baz")).
		Bind(BASIC_BLOB.BLOB_COLUMN.To(&b)).
		FetchOne(session)

	if found && bytes.Equal(blob.BlobColumn, b) {
		result = "PASSED"
	} else {
		result = fmt.Sprintf("Blob was %s", string(b))
	}

	os.Stdout.WriteString(result)
}
