package generator

import (
	"errors"
	"github.com/gocql/gocql"
	"github.com/relops/cqlc/meta"
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

var literalTypes = map[gocql.Type]string{
	gocql.TypeAscii:     "string",
	gocql.TypeVarchar:   "string",
	gocql.TypeInt:       "int32",
	gocql.TypeBigInt:    "int64",
	gocql.TypeFloat:     "float32",
	gocql.TypeDouble:    "float64",
	gocql.TypeTimestamp: "time.Time",
	gocql.TypeTimeUUID:  "gocql.UUID",
	gocql.TypeUUID:      "gocql.UUID",
	gocql.TypeBoolean:   "bool",
	gocql.TypeCounter:   "int64",
	gocql.TypeBlob:      "[]byte",
	gocql.TypeDecimal:   "*inf.Dec",
	gocql.TypeVarint:    "*big.Int",
}

var customImportPaths = map[string]string{
	"gocql.UUID": "github.com/gocql/gocql",
	"*inf.Dec":   "speter.net/go/exp/math/dec/inf",
	"*big.Int":   "math/big",
}

var columnTypes = map[gocql.Type]string{
	gocql.TypeAscii:     "cqlc.String_Column",
	gocql.TypeVarchar:   "cqlc.String_Column",
	gocql.TypeInt:       "cqlc.Int32_Column",
	gocql.TypeBigInt:    "cqlc.Int64_Column",
	gocql.TypeFloat:     "cqlc.Float32_Column",
	gocql.TypeDouble:    "cqlc.Float64_Column",
	gocql.TypeTimestamp: "cqlc.Timestamp_Column",
	gocql.TypeTimeUUID:  "cqlc.TimeUUID_Column",
	gocql.TypeUUID:      "cqlc.UUID_Column",
	gocql.TypeBoolean:   "cqlc.Boolean_Column",
	gocql.TypeCounter:   "cqlc.Counter_Column",
	gocql.TypeBlob:      "cqlc.Bytes_Column",
	gocql.TypeDecimal:   "cqlc.Decimal_Column",
	gocql.TypeVarint:    "cqlc.Varint_Column",
}
