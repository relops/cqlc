package cqlc

type MapColumn interface {
	Column
}

type ArrayColumn interface {
	Column
}

type CounterColumn interface {
	Column
	CanIncrement() bool
	To(value *int64) ColumnBinding
}
