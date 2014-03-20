package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"github.com/relops/csvb"
	"log"
	"os"
)

func main() {

	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, USER_ACCOUNTS)

	result := "FAILED"

	ctx := cqlc.NewContext()
	batch := gocql.NewBatch(gocql.LoggedBatch)

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Could not open CSV file: %s", err)
		os.Exit(1)
	}

	b, err := csvb.NewBinder(f, nil)
	if err != nil {
		log.Fatalf("Could not create binder: %s", err)
		os.Exit(1)
	}

	s := make(map[string]string)
	s["username"] = "Username"
	s["email"] = "Email"
	s["password"] = "Password"
	s["last_visited"] = "LastVisited"
	s["country"] = "Country"

	err = b.ForEach(func(row csvb.Row) (bool, error) {
		var u UserAccounts

		if err := row.Bind(&u, s); err != nil {
			return false, err
		}

		ctx.Store(USER_ACCOUNTS.Bind(u)).Batch(batch)

		return true, nil
	})

	if err != nil {
		log.Fatalf("Could not bind CSV file: %s", err)
		os.Exit(1)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Fatalf("Could not execute batch: %v", err)
		os.Exit(1)
	}

	iter, err := ctx.Select().
		From(USER_ACCOUNTS).
		Where(USER_ACCOUNTS.COUNTRY.Eq("uk")).
		Fetch(session)

	if err != nil {
		log.Fatalf("Could not query by secondary index: %s", err)
		os.Exit(1)
	}

	count := 0
	err = MapUserAccounts(iter, func(u UserAccounts) (bool, error) {
		count++
		if u.Country != "uk" {
			return false, fmt.Errorf("Expected country %s; got %s", "uk", u.Country)
		}
		return true, nil
	})

	if err != nil {
		log.Fatalf("Could not query by secondary index: %s", err)
		os.Exit(1)
	}

	if count == 1 {
		result = "PASSED"
	}

	os.Stdout.WriteString(result)
}
