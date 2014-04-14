package meta

type ColumnDataType uint

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
	SliceType
	BytesType
	DecimalType
	ReversedType
	BasicType
	NoType
)

var DataTypes = map[string]ColumnDataType{
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
