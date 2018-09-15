package e2e

import (
	"log"
	"testing"

	"github.com/gocql/gocql"
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/e2e/t1gen"
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

		// NOTE: the table name need to have some lower case to avoid conflict .... if we use t1, it will break ...
		createTable := `
CREATE TABLE cqlc.t1abc (
    id text PRIMARY KEY,
    ts timestamp,
    string_map map<text, text>,
    string_list list<text>,
)
`
		err = sess.Query(createTable).Exec()
		require.Nil(err, "create table")

		// cqlc --instance=127.0.0.1 --keyspace=cqlc --package=t1gen --output=t1gen/generated.go --verbose --symbols
	})

	t.Run("insert", func(t *testing.T) {
		require := requir.New(t)

		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = "cqlc"
		sess, err := cluster.CreateSession()
		require.Nil(err, "connect to cassandra using cqlc keyspace for create new table")

		c := cqlc.NewContext()
		c.Debug = true
		// FIXME: StringMapValue didn't work "can not marshal []interface {} into map(varchar, varchar)"
		err = c.Upsert(t1gen.T1abcTableDef()).
			SetString(t1gen.T1ABC.ID, "2").
			//SetStringStringMapValue(t1gen.T1ABC.STRING_MAP, "1", "2").Exec(sess)
			SetStringStringMap(t1gen.T1ABC.STRING_MAP, map[string]string{"1": "2"}).
			Exec(sess)
		require.Nil(err, "insert map")

		err = c.Upsert(t1gen.T1abcTableDef()).
			SetStringStringMapValue(t1gen.T1ABC.STRING_MAP, "1", "4").
			Where(t1gen.T1ABC.ID.Eq("2")).
			Exec(sess)
		//stmt, holders, err := cqlc.BuildStatement(c)
		//t.Log(stmt)
		//t.Log(holders)
		require.Nil(err, "update map")

		var row t1gen.T1abc

		found, err := c.Select(t1gen.T1ABC.STRING_MAP).From(t1gen.T1ABC).
			Where(t1gen.T1ABC.ID.Eq("2")).
			Into(t1gen.T1ABC.To(&row)).FetchOne(sess)
		require.Nil(err)
		require.True(found, "found the row")
		require.Equal("4", row.StringMap["1"])

		//err = c.Upsert(t1gen.T1abcTableDef()).
		//	SetString(t1gen.T1ABC.ID, "1").
		//	SetTimestamp(t1gen.T1ABC.TS, time.Now()).Exec(sess)
		//require.Nil(err, "insert map")
	})
}

func init() {
	log.SetFlags(log.Lshortfile)
}
