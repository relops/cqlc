package cqlc

type CounterColumn interface {
	Column
	CanIncrement() bool
	To(value *int64) ColumnBinding
}
