package generator

import (
	"fmt"
	"github.com/gocql/gocql"
	"sort"
	"strings"
)

type ColumnKeyType uint
type ColumnDataType uint

const (
	PartitionKey ColumnKeyType = iota
	ClusteringKey
	RegularColumn
)

const (
	StringType ColumnDataType = iota
	Int32Type
	LongType
	FloatType
	DoubleType
	TimestampType
	BooleanType
	TimeUUIDType
	UUIDType
	CounterType
	MapType
	ArrayType
	BytesType
	DecimalType
	ReversedType
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
	"org.apache.cassandra.db.marshal.UUIDType":          UUIDType,
	"org.apache.cassandra.db.marshal.TimeUUIDType":      TimeUUIDType,
	"org.apache.cassandra.db.marshal.BooleanType":       BooleanType,
	"org.apache.cassandra.db.marshal.CounterColumnType": CounterType,
	"org.apache.cassandra.db.marshal.BytesType":         BytesType,
	"org.apache.cassandra.db.marshal.DecimalType":       DecimalType,
}

var templateDataTypes = map[string]ColumnDataType{
	"org.apache.cassandra.db.marshal.MapType":      MapType,
	"org.apache.cassandra.db.marshal.ListType":     ArrayType,
	"org.apache.cassandra.db.marshal.SetType":      ArrayType,
	"org.apache.cassandra.db.marshal.ReversedType": ReversedType,
}

var literalTypes = map[ColumnDataType]string{
	StringType:    "string",
	Int32Type:     "int32",
	LongType:      "int64",
	FloatType:     "float32",
	DoubleType:    "float64",
	TimestampType: "time.Time",
	TimeUUIDType:  "gocql.UUID",
	UUIDType:      "gocql.UUID",
	BooleanType:   "bool",
	CounterType:   "int64",
	MapType:       "map[string]string",
	ArrayType:     "[]string",
	BytesType:     "[]byte",
	DecimalType:   "*inf.Dec",
}

var customImportPaths = map[string]string{
	"gocql.UUID": "github.com/gocql/gocql",
	"*inf.Dec":   "speter.net/go/exp/math/dec/inf",
}

var columnTypes = map[ColumnDataType]string{
	StringType:    "cqlc.StringColumn",
	Int32Type:     "cqlc.Int32Column",
	LongType:      "cqlc.Int64Column",
	FloatType:     "cqlc.Float32Column",
	DoubleType:    "cqlc.Float64Column",
	TimestampType: "cqlc.TimestampColumn",
	TimeUUIDType:  "cqlc.TimeUUIDColumn",
	UUIDType:      "cqlc.UUIDColumn",
	BooleanType:   "cqlc.BooleanColumn",
	CounterType:   "cqlc.CounterColumn",
	MapType:       "cqlc.MapColumn",
	ArrayType:     "cqlc.ArrayColumn",
	BytesType:     "cqlc.BytesColumn",
	DecimalType:   "cqlc.DecimalColumn",
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
	SecondaryIndex  bool
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

func ColumnFamilies(opts *Options) ([]ColumnFamily, error) {
	verbose := len(opts.Verbose) > 0
	cluster := gocql.NewCluster(opts.Instance)

	if opts.Username != "" && opts.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: opts.Username,
			Password: opts.Password,
		}
	}

	session, err := cluster.CreateSession()

	if err != nil {
		fmt.Errorf("Connect error", err)
	}

	fmt.Printf("Reading schema from keyspace: %s\n", opts.Keyspace)

	iter := session.Query(`SELECT columnfamily_name
                           FROM system.schema_columnfamilies
                           WHERE keyspace_name = ?`, opts.Keyspace).Iter()

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

		if verbose {
			fmt.Printf("Reading metadata for %s.%s ...\n", opts.Keyspace, cf.Name)
		}

		iter := session.Query(`SELECT column_name, type, validator, component_index, index_name
                               FROM system.schema_columns
                               WHERE keyspace_name = ? AND columnfamily_name = ?`, opts.Keyspace, cf.Name).Iter()
		columns := make([]Column, 0)
		var col Column
		var colKeyType, validator, secondaryIndex string
		for iter.Scan(&col.Name, &colKeyType, &validator, &col.ComponentIndex, &secondaryIndex) {
			col.KeyType = keyTypes[colKeyType]
			dataType, ok := dataTypes[validator]

			if !ok {
				// TODO This is extremely hacky, must clean this up
				// Basically a map<text,text> type will come through as:
				// org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)
				parts := strings.Split(validator, "(")
				if len(parts) == 0 {
					// TODO should error out here really, since we can't map the type
					fmt.Printf("Unmapped data type: %s\n", validator)
				}

				dataType = templateDataTypes[parts[0]]
				switch dataType {
				case ReversedType:
					{
						// Ugly hack to get reversed columns going
						s := strings.Replace(parts[1], ")", "", -1)
						dataType, ok = dataTypes[s]
						if !ok {
							fmt.Printf("Unmapped data type: %s\n", validator)
						}
					}
				case 0:
					{
						// TODO should error out here really, since we can't map the type
						fmt.Printf("Unmapped data type: %s\n", validator)
					}
				}
			}

			col.DataType = dataType

			if col.DataType == CounterType {
				columnFamilies[i].IsCounter = true
			}

			if secondaryIndex == "" {
				col.SecondaryIndex = false
			} else {
				col.SecondaryIndex = true
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

			if !foundClustered {
				if columns[i].SupportsClustering() {
					columns[i].IsLastComponent = true
					foundClustered = true
				}
			}

			if !foundParitioned {
				if columns[i].SupportsPartitioning() {
					columns[i].IsLastComponent = true
					foundParitioned = true
				}
			}
		}

		if verbose {
			for _, col := range columns {
				fmt.Printf("[%s.%s] Column: %+v\n", opts.Keyspace, cf.Name, col)
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
