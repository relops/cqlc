package generator

import (
	"fmt"
	"github.com/tux21b/gocql"
	"sort"
	"strings"
)

type ColumnKeyType uint
type ColumnDataType uint

const (
	PartitionKey  ColumnKeyType = 1
	ClusteringKey ColumnKeyType = 2
	RegularColumn ColumnKeyType = 3
)

const (
	StringType    ColumnDataType = 1
	Int32Type     ColumnDataType = 2
	LongType      ColumnDataType = 3
	FloatType     ColumnDataType = 4
	DoubleType    ColumnDataType = 5
	TimestampType ColumnDataType = 6
	BooleanType   ColumnDataType = 7
	TimeUUIDType  ColumnDataType = 8
	CounterType   ColumnDataType = 9
	MapType       ColumnDataType = 10
	ArrayType     ColumnDataType = 11
)

var keyTypes = map[string]ColumnKeyType{
	"partition_key":  PartitionKey,
	"clustering_key": ClusteringKey,
	"regular":        RegularColumn,
}

var dataTypes = map[string]ColumnDataType{
	"org.apache.cassandra.db.marshal.AsciiType":         StringType,
	"org.apache.cassandra.db.marshal.UTF8Type":          StringType,
	"org.apache.cassandra.db.marshal.Int32Type":         Int32Type,
	"org.apache.cassandra.db.marshal.LongType":          LongType,
	"org.apache.cassandra.db.marshal.FloatType":         FloatType,
	"org.apache.cassandra.db.marshal.DoubleType":        DoubleType,
	"org.apache.cassandra.db.marshal.TimestampType":     TimestampType,
	"org.apache.cassandra.db.marshal.TimeUUIDType":      TimeUUIDType,
	"org.apache.cassandra.db.marshal.BooleanType":       BooleanType,
	"org.apache.cassandra.db.marshal.CounterColumnType": CounterType,
}

var collectionDataTypes = map[string]ColumnDataType{
	"org.apache.cassandra.db.marshal.MapType":  MapType,
	"org.apache.cassandra.db.marshal.ListType": ArrayType,
}

var literalTypes = map[ColumnDataType]string{
	StringType:    "string",
	Int32Type:     "int32",
	LongType:      "int64",
	FloatType:     "float32",
	DoubleType:    "float64",
	TimestampType: "time.Time",
	TimeUUIDType:  "gocql.UUID",
	BooleanType:   "bool",
	CounterType:   "int64",
	MapType:       "map[string]string",
	ArrayType:     "[]string",
}

var customImportPaths = map[string]string{
	"gocql.UUID": "github.com/tux21b/gocql",
}

var columnTypes = map[ColumnDataType]string{
	StringType:    "cqlc.StringColumn",
	Int32Type:     "cqlc.Int32Column",
	LongType:      "cqlc.Int64Column",
	FloatType:     "cqlc.Float32Column",
	DoubleType:    "cqlc.Float64Column",
	TimestampType: "cqlc.TimestampColumn",
	TimeUUIDType:  "cqlc.TimeUUIDColumn",
	BooleanType:   "cqlc.BooleanColumn",
	CounterType:   "cqlc.CounterColumn",
	MapType:       "cqlc.MapColumn",
	ArrayType:     "cqlc.ArrayColumn",
}

type Binding struct {
	Name string
}

type ColumnFamily struct {
	Name      string
	Columns   []Column
	IsCounter bool
}

type Column struct {
	Name            string
	KeyType         ColumnKeyType
	DataType        ColumnDataType
	ComponentIndex  int
	IsLastComponent bool
}

type ByComponentIndex []Column

func (a ByComponentIndex) Len() int           { return len(a) }
func (a ByComponentIndex) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByComponentIndex) Less(i, j int) bool { return a[i].ComponentIndex < a[j].ComponentIndex }

func (c *Column) SupportsPartitioning() bool {
	return c.KeyType == PartitionKey
}

func (c *Column) SupportsClustering() bool {
	return c.KeyType == ClusteringKey
}

func ColumnFamilies(host string, keyspace string) ([]ColumnFamily, error) {

	cluster := gocql.NewCluster(host)
	session, err := cluster.CreateSession()

	if err != nil {
		fmt.Errorf("Connect error", err)
	}

	fmt.Printf("Reading schema from keyspace: %s\n", keyspace)

	iter := session.Query(`SELECT columnfamily_name 
                           FROM system.schema_columnfamilies
                           WHERE keyspace_name = ?`, keyspace).Iter()

	columnFamilies := make([]ColumnFamily, 0)
	var cf ColumnFamily
	for iter.Scan(&cf.Name) {
		columnFamilies = append(columnFamilies, cf)
	}

	err = iter.Close()
	if err != nil {
		fmt.Errorf("Read error", err)
	}

	for i, cf := range columnFamilies {
		iter := session.Query(`SELECT column_name, type, validator, component_index 
                               FROM system.schema_columns
                               WHERE keyspace_name = ? AND columnfamily_name = ?`, keyspace, cf.Name).Iter()
		columns := make([]Column, 0)
		var col Column
		var colKeyType, validator string
		for iter.Scan(&col.Name, &colKeyType, &validator, &col.ComponentIndex) {
			col.KeyType = keyTypes[colKeyType]
			dataType, present := dataTypes[validator]

			if !present {
				// TODO This is extremely hacky, must clean this up
				// Basically a map<text,text> type will come through as:
				// org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)
				parts := strings.Split(validator, "(")
				if len(parts) == 0 {
					// TODO should error out here really, since we can't map the type
					fmt.Printf("Unmapped data type: %s\n", validator)
				}
				dataType = collectionDataTypes[parts[0]]
				if dataType == 0 {
					// TODO should error out here really, since we can't map the type
					fmt.Printf("Unmapped data type: %s\n", validator)
				}
			}

			col.DataType = dataType

			if col.DataType == CounterType {
				columnFamilies[i].IsCounter = true
			}

			columns = append(columns, col)
		}

		sort.Sort(sort.Reverse(ByComponentIndex(columns)))

		foundParitioned := false
		foundClustered := false

		for i, _ := range columns {
			if foundClustered && foundParitioned {
				break
			}
			if foundClustered || foundParitioned {
				continue
			}
			if columns[i].SupportsClustering() {
				columns[i].IsLastComponent = true
				foundClustered = true
			}
			if columns[i].SupportsPartitioning() {
				columns[i].IsLastComponent = true
				foundParitioned = true
			}

		}

		columnFamilies[i].Columns = columns
	}

	err = iter.Close()
	if err != nil {
		fmt.Printf("Read error", err)
	}

	session.Close()

	return columnFamilies, err
}
