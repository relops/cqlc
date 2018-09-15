package e2e

import (
	"testing"

	"github.com/gocql/gocql"
	requir "github.com/stretchr/testify/require"
)

func TestCreateSchema(t *testing.T) {
	t.Run("create keyspace", func(t *testing.T) {
		require := requir.New(t)

		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = "system"
		sess, err := cluster.CreateSession()
		require.Nil(err, "connect to cassandra using system keyspace for create new keyspace")

		createKeyspace := `CREATE KEYSPACE cqlc WITH replication = 
{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`
		err = sess.Query(createKeyspace).Exec()
		require.Nil(err, "create keyspace")
	})

	t.Run("create table", func(t *testing.T) {
		require := requir.New(t)

		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = "cqlc"
		sess, err := cluster.CreateSession()
		require.Nil(err, "connect to cassandra using cqlc keyspace for create new table")

		createTable := `
CREATE TABLE cqlc.t1 (
    id text PRIMARY KEY,
    ts timestamp,
    string_map map<text, text>,
    string_list list<text>,
)
`
		err = sess.Query(createTable).Exec()
		require.Nil(err, "create table")
	})
}
