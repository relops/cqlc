package e2e

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"

	"github.com/gocql/gocql"
	asst "github.com/stretchr/testify/assert"
	requir "github.com/stretchr/testify/require"

	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/e2e/g1"
)

const (
	cqlcKs  = "cqlc"
	gocqlKs = "gocql"
)

func TestCreateKeyspace(t *testing.T) {
	require := requir.New(t)

	// create two ks, one for cqlc, one for gocql, so table for gocql won't get dumped by cqlc generator
	for _, ks := range []string{cqlcKs, gocqlKs} {
		createKeyspace := fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, ks)

		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = "system"
		sess, err := cluster.CreateSession()
		require.Nil(err, "connect to cassandra using system keyspace for create new keyspace"+ks)
		err = sess.Query(createKeyspace).Exec()
		require.Nil(err, "create keyspace")
		log.Printf("created keyspace %s", ks)
	}
}

const tblStringMap = `
CREATE TABLE %s (
    id text PRIMARY KEY,
    ssm map<text, text>
)
`

const tblMapSlice = `
CREATE TABLE %s (
    id text PRIMARY KEY,
    ssm map<text, text>,
	sl list<text>
)
`

// for each ddl, we create two table, one for gocql, one for cqlc
func TestCreateTable(t *testing.T) {
	require := requir.New(t)

	for _, ks := range []string{cqlcKs, gocqlKs} {
		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = ks
		sess, err := cluster.CreateSession()
		require.Nil(err, "connect to cassandra using cqlc keyspace for create new table")

		ddls := []string{tblStringMap, tblMapSlice}
		tbs := []string{"tbl_string_map", "tbl_map_slice"}
		for i := 0; i < len(tbs); i++ {
			err = sess.Query(fmt.Sprintf(ddls[i], tbs[i])).Exec()
			require.Nil(err, "create table "+tbs[i])
			log.Printf("created %s.%s", ks, tbs[i])
		}
	}
}

func TestGenerate(t *testing.T) {
	// cqlc --instance=127.0.0.1 --keyspace=cqlc --package=g1 --output=g1/cqlc_generated.go --verbose --symbols
	runCqlc(t, cqlcKs, "g1")
}

func TestUpdate(t *testing.T) {
	require := requir.New(t)
	assert := asst.New(t)

	sf := func(ks string) *gocql.Session {
		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = ks
		sess, err := cluster.CreateSession()
		require.Nil(err, "connect to cassandra using cqlc keyspace for create new table")
		return sess
	}

	sGocql := sf(gocqlKs)
	sCqlc := sf(cqlcKs)

	t.Run("insert", func(t *testing.T) {
		err := sGocql.Query("INSERT INTO tbl_string_map (id, ssm) VALUES (?,?)", "1", map[string]string{"k1": "v1"}).Exec()
		require.Nil(err)

		c := cqlc.NewContext()
		c.Debug = true
		err = c.Upsert(g1.TblStringMapTableDef()).SetString(g1.TBL_STRING_MAP.ID, "1").
			SetStringStringMap(g1.TBL_STRING_MAP.SSM, map[string]string{"k1": "v1"}).Exec(sCqlc)
		require.Nil(err)
	})

	t.Run("update", func(t *testing.T) {
		err := sGocql.Query("UPDATE tbl_string_map SET ssm[?] = ? WHERE id = ?", "k1", "v2", "1").Exec()
		require.Nil(err)

		c := cqlc.NewContext()
		c.Debug = true
		err = c.Upsert(g1.TblStringMapTableDef()).
			SetStringStringMapValue(g1.TBL_STRING_MAP.SSM, "k1", "v2").
			Where(g1.TBL_STRING_MAP.ID.Eq("1")).Exec(sCqlc)
		require.Nil(err)

	})

	t.Run("query", func(t *testing.T) {
		iter := sGocql.Query("SELECT id, ssm FROM tbl_string_map WHERE id = ?", "1").Iter()
		var id string
		var m = map[string]string{}
		scanned := iter.Scan(&id, &m)
		assert.True(scanned)
		require.Nil(iter.Close())
		require.Equal("1", id)
		require.Equal("v2", m["k1"])

		c := cqlc.NewContext()
		c.Debug = true
		var row g1.TblStringMap
		ok, err := c.Select(g1.TBL_STRING_MAP.ID, g1.TBL_STRING_MAP.SSM).From(g1.TblStringMapTableDef()).
			Where(g1.TBL_STRING_MAP.ID.Eq("1")).Into(g1.TBL_STRING_MAP.To(&row)).FetchOne(sCqlc)
		require.Nil(err)
		assert.True(ok)
		require.Equal("1", row.Id)
		require.Equal("v2", row.Ssm["k1"])
	})
}

func runCqlc(t *testing.T, ks string, pkg string) {
	// cqlc --instance=127.0.0.1 --keyspace=cqlc --package=t1gen --output=t1gen/generated.go --verbose --symbols
	cmd := exec.Command("cqlc",
		"--instance=127.0.0.1",
		"--keyspace="+ks,
		"--package="+pkg,
		"--output="+pkg+"/cqlc_generated.go",
		"--symbols",
		"--verbose",
	)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	log.Printf("cqlc generate ks %s to %s", ks, pkg)
}

func init() {
	log.SetFlags(log.Lshortfile)
}
