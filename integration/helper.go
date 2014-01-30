package integration

import (
	"fmt"
	"github.com/relops/cqlc/cqlc"
	"log"
	"os"
	"tux21b.org/v1/gocql"
)

func TestSession(host string, keyspace string) *gocql.Session {
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to cassandra: %v", err)
		os.Exit(1)
	}
	return session
}

func Truncate(session *gocql.Session, table cqlc.Table) {
	stmt := fmt.Sprintf("truncate %s", table.TableName())
	err := session.Query(stmt).Exec()

	if err != nil {
		log.Fatalf("Could not %s: %v", stmt, err)
		os.Exit(1)
	}
}
