package main

import (
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"log"
	"os"
	"reflect"
)

func main() {

	s := integration.TestSession("127.0.0.1", "cqlc")
	cqlc.Truncate(s, COLLECTIONS)

	result := "FAILED"

	ctx := cqlc.NewContext()

	if err := ctx.Upsert(COLLECTIONS).AppendInt32Slice(COLLECTIONS.INT32_COLUMN, 1, 2).Where(COLLECTIONS.ID.Eq(20)).Exec(s); err != nil {
		log.Fatalf("Could not increment collections: %v", err)
		os.Exit(1)
	}

	var output []int32
	found, err := ctx.Select(COLLECTIONS.INT32_COLUMN).
		From(COLLECTIONS).
		Where(COLLECTIONS.ID.Eq(20)).
		Bind(COLLECTIONS.INT32_COLUMN.To(&output)).
		FetchOne(s)
	if err != nil {
		log.Fatalf("Could not retreive collections: %v", err)
		os.Exit(1)
	}

	if found {
		if reflect.DeepEqual([]int32{1, 2}, output) {
			if err := ctx.Upsert(COLLECTIONS).AppendInt32Slice(COLLECTIONS.INT32_COLUMN, 3, 4).Where(COLLECTIONS.ID.Eq(20)).Exec(s); err != nil {
				log.Fatalf("Could not increment collections: %v", err)
				os.Exit(1)
			}

			var output []int32
			found, err := ctx.Select(COLLECTIONS.INT32_COLUMN).
				From(COLLECTIONS).
				Where(COLLECTIONS.ID.Eq(20)).
				Bind(COLLECTIONS.INT32_COLUMN.To(&output)).
				FetchOne(s)
			if err != nil {
				log.Fatalf("Could not retreive collections: %v", err)
				os.Exit(1)
			}

			if found {
				if reflect.DeepEqual([]int32{1, 2, 3, 4}, output) {

					if err := ctx.Upsert(COLLECTIONS).PrependInt32Slice(COLLECTIONS.INT32_COLUMN, 0).Where(COLLECTIONS.ID.Eq(20)).Exec(s); err != nil {
						log.Fatalf("Could not increment collections: %v", err)
						os.Exit(1)
					}

					var output []int32
					found, err := ctx.Select(COLLECTIONS.INT32_COLUMN).
						From(COLLECTIONS).
						Where(COLLECTIONS.ID.Eq(20)).
						Bind(COLLECTIONS.INT32_COLUMN.To(&output)).
						FetchOne(s)
					if err != nil {
						log.Fatalf("Could not retreive collections: %v", err)
						os.Exit(1)
					}

					if found {

						if reflect.DeepEqual([]int32{0, 1, 2, 3, 4}, output) {

							if err := ctx.Upsert(COLLECTIONS).RemoveInt32Slice(COLLECTIONS.INT32_COLUMN, 3, 1).Where(COLLECTIONS.ID.Eq(20)).Exec(s); err != nil {
								log.Fatalf("Could not increment collections: %v", err)
								os.Exit(1)
							}

							var output []int32
							found, err := ctx.Select(COLLECTIONS.INT32_COLUMN).
								From(COLLECTIONS).
								Where(COLLECTIONS.ID.Eq(20)).
								Bind(COLLECTIONS.INT32_COLUMN.To(&output)).
								FetchOne(s)
							if err != nil {
								log.Fatalf("Could not retreive collections: %v", err)
								os.Exit(1)
							}

							if found {

								if reflect.DeepEqual([]int32{0, 2, 4}, output) {
									result = "PASSED"
								}
							}

						}
					}
				}
			}
		}
	}

	os.Stdout.WriteString(result)
}
