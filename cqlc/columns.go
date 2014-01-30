package cqlc

import (
	"time"
	"tux21b.org/v1/gocql/uuid"
)

type StringColumn interface {
	Column
	Supports(value string) bool
}

type PartitionedStringColumn interface {
	StringColumn
	Eq(value string) Condition
}

type LastPartitionedStringColumn interface {
	PartitionedStringColumn
	In(value ...string) Condition
}

type ClusteredStringColumn interface {
	PartitionedStringColumn
	Gt(value string) Condition
	Lt(value string) Condition
	Ge(value string) Condition
	Le(value string) Condition
}

type LastClusteredStringColumn interface {
	ClusteredStringColumn
	In(value ...string) Condition
}

type Int32Column interface {
	Column
	Supports(value int32) bool
}

type PartitionedInt32Column interface {
	Int32Column
	Eq(value int32) Condition
}

type LastPartitionedInt32Column interface {
	PartitionedInt32Column
	In(value ...int32) Condition
}

type ClusteredInt32Column interface {
	PartitionedInt32Column
	Gt(value int32) Condition
	Lt(value int32) Condition
	Ge(value int32) Condition
	Le(value int32) Condition
}

type LastClusteredInt32Column interface {
	ClusteredInt32Column
	In(value ...int32) Condition
}

type Int64Column interface {
	Column
	Supports(value int64) bool
}

type PartitionedInt64Column interface {
	Int64Column
	Eq(value int64) Condition
}

type LastPartitionedInt64Column interface {
	PartitionedInt64Column
	In(value ...int64) Condition
}

type ClusteredInt64Column interface {
	PartitionedInt64Column
	Gt(value int64) Condition
	Lt(value int64) Condition
	Ge(value int64) Condition
	Le(value int64) Condition
}

type LastClusteredInt64Column interface {
	ClusteredInt64Column
	In(value ...int64) Condition
}

type Float32Column interface {
	Column
	Supports(value float32) bool
}

type PartitionedFloat32Column interface {
	Float32Column
	Eq(value float32) Condition
}

type LastPartitionedFloat32Column interface {
	PartitionedFloat32Column
	In(value ...float32) Condition
}

type ClusteredFloat32Column interface {
	PartitionedFloat32Column
	Gt(value float32) Condition
	Lt(value float32) Condition
	Ge(value float32) Condition
	Le(value float32) Condition
}

type LastClusteredFloat32Column interface {
	ClusteredFloat32Column
	In(value ...float32) Condition
}

type Float64Column interface {
	Column
	Supports(value float64) bool
}

type PartitionedFloat64Column interface {
	Float64Column
	Eq(value float64) Condition
}

type LastPartitionedFloat64Column interface {
	PartitionedFloat64Column
	In(value ...float64) Condition
}

type ClusteredFloat64Column interface {
	PartitionedFloat64Column
	Gt(value float64) Condition
	Lt(value float64) Condition
	Ge(value float64) Condition
	Le(value float64) Condition
}

type LastClusteredFloat64Column interface {
	ClusteredFloat64Column
	In(value ...float64) Condition
}

type TimestampColumn interface {
	Column
	Supports(value time.Time) bool
}

type PartitionedTimestampColumn interface {
	TimestampColumn
	Eq(value time.Time) Condition
}

type LastPartitionedTimestampColumn interface {
	PartitionedTimestampColumn
	In(value ...time.Time) Condition
}

type ClusteredTimestampColumn interface {
	PartitionedTimestampColumn
	Gt(value time.Time) Condition
	Lt(value time.Time) Condition
	Ge(value time.Time) Condition
	Le(value time.Time) Condition
}

type LastClusteredTimestampColumn interface {
	ClusteredTimestampColumn
	In(value ...time.Time) Condition
}

type TimeUUIDColumn interface {
	Column
	Supports(value uuid.UUID) bool
}

type PartitionedTimeUUIDColumn interface {
	TimeUUIDColumn
	Eq(value uuid.UUID) Condition
}

type LastPartitionedTimeUUIDColumn interface {
	PartitionedTimeUUIDColumn
	In(value ...uuid.UUID) Condition
}

type ClusteredTimeUUIDColumn interface {
	PartitionedTimeUUIDColumn
	Gt(value uuid.UUID) Condition
	Lt(value uuid.UUID) Condition
	Ge(value uuid.UUID) Condition
	Le(value uuid.UUID) Condition
}

type LastClusteredTimeUUIDColumn interface {
	ClusteredTimeUUIDColumn
	In(value ...uuid.UUID) Condition
}

type BooleanColumn interface {
	Column
	Supports(value bool) bool
}

type PartitionedBooleanColumn interface {
	BooleanColumn
	Eq(value bool) Condition
}

type LastPartitionedBooleanColumn interface {
	PartitionedBooleanColumn
	In(value ...bool) Condition
}

type ClusteredBooleanColumn interface {
	PartitionedBooleanColumn
	Gt(value bool) Condition
	Lt(value bool) Condition
	Ge(value bool) Condition
	Le(value bool) Condition
}

type LastClusteredBooleanColumn interface {
	ClusteredBooleanColumn
	In(value ...bool) Condition
}

type MapColumn interface {
	Column
}

type ArrayColumn interface {
	Column
}

type CounterColumn interface {
	Column
	CanIncrement() bool
}
