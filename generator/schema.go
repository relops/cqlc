package generator

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/meta"
	"sort"
)

var (
	ErrTypeUnknown = errors.New("unknown data type")
)

type ColumnKeyType uint

const (
	PartitionKey ColumnKeyType = iota
	ClusteringKey
	RegularColumn
)

var keyTypes = map[string]ColumnKeyType{
	"partition_key":  PartitionKey,
	"clustering_key": ClusteringKey,
	"regular":        RegularColumn,
}

type ColumnDataInfo struct {
	DomainType  meta.ColumnDataType
	RangeType   meta.ColumnDataType
	GenericType meta.ColumnDataType
}

var literalTypes = map[meta.ColumnDataType]string{
	meta.StringType:    "string",
	meta.Int32Type:     "int32",
	meta.LongType:      "int64",
	meta.FloatType:     "float32",
	meta.DoubleType:    "float64",
	meta.TimestampType: "time.Time",
	meta.TimeUUIDType:  "gocql.UUID",
	meta.UUIDType:      "gocql.UUID",
	meta.BooleanType:   "bool",
	meta.CounterType:   "int64",
	meta.MapType:       "map[string]string",
	meta.BytesType:     "[]byte",
	meta.DecimalType:   "*inf.Dec",
	meta.VarintType:    "*big.Int",
}

var customImportPaths = map[string]string{
	"gocql.UUID": "github.com/gocql/gocql",
	"*inf.Dec":   "speter.net/go/exp/math/dec/inf",
	"*big.Int":   "math/big",
}

var columnTypes = map[meta.ColumnDataType]string{
	meta.StringType:    "cqlc.String_Column",
	meta.Int32Type:     "cqlc.Int32_Column",
	meta.LongType:      "cqlc.Int64_Column",
	meta.FloatType:     "cqlc.Float32_Column",
	meta.DoubleType:    "cqlc.Float64_Column",
	meta.TimestampType: "cqlc.Timestamp_Column",
	meta.TimeUUIDType:  "cqlc.TimeUUID_Column",
	meta.UUIDType:      "cqlc.UUID_Column",
	meta.BooleanType:   "cqlc.Boolean_Column",
	meta.CounterType:   "cqlc.Counter_Column",
	meta.MapType:       "cqlc.Map_Column",
	meta.BytesType:     "cqlc.Bytes_Column",
	meta.DecimalType:   "cqlc.Decimal_Column",
	meta.VarintType:    "cqlc.Varint_Column",
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
	DataInfo        ColumnDataInfo
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

func (c *Column) IsListType() bool {
	return c.DataInfo.GenericType == meta.SliceType
}

func ColumnFamilies(session *gocql.Session, opts *Options) ([]ColumnFamily, error) {
	verbose := len(opts.Verbose) > 0

	fmt.Printf("Reading schema from keyspace: %s\n", opts.Keyspace)

	iter := session.Query(`SELECT columnfamily_name
                           FROM system.schema_columnfamilies
                           WHERE keyspace_name = ?`, opts.Keyspace).Iter()

	columnFamilies := make([]ColumnFamily, 0)
	var cf ColumnFamily
	for iter.Scan(&cf.Name) {
		columnFamilies = append(columnFamilies, cf)
	}

	err := iter.Close()
	if err != nil {
		fmt.Errorf("Read error: %s", err)
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

			dataInfo, err := ParseValidator(validator)

			if err != nil {
				// TODO Should we not exit here?
				fmt.Printf("Unmapped data type: %s\n", validator)
			}

			col.DataInfo = dataInfo

			if col.DataInfo.DomainType == meta.CounterType {
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
