// cqlc generates Go code from your Cassandra schema
// so that you can write type safe CQL statements in Go with a natural query syntax.
//
// For a full guide visit http://relops.com/cqlc
//
//  var FOO = FooTableDef()
//
//  iter, err := ctx.Select(FOO.BAR).
//                   From(FOO).
//                   Where(FOO.BAZ.Eq("x")).
//                   Fetch(session)
//
//  var foos []Foo = BindFoos(iter)
package cqlc

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type OperationType int
type PredicateType int
type CollectionType int
type CollectionOperationType int

const (
	EqPredicate PredicateType = iota
	GtPredicate
	GePredicate
	LtPredicate
	LePredicate
	InPredicate
)

const (
	None OperationType = iota
	ReadOperation
	WriteOperation
	DeleteOperation
	CounterOperation
)

const (
	NoCollectionType CollectionType = iota
	ListType
	SetType
	MapType
)

const (
	Noop CollectionOperationType = iota
	Append
	Prepend
	RemoveByValue
	RemoveByKey
	SetByKey
)

var (
	ErrCASBindings = errors.New("Invalid CAS bindings")
)

type OrderSpec struct {
	Col  string
	Desc bool
}

type ReadOptions struct {
	Distinct bool
	Limit    int
	Ordering []OrderSpec
}

// Context represents the state of the CQL statement that is being built by the application.
type Context struct {
	Operation   OperationType
	Table       Table
	Columns     []Column
	Bindings    []ColumnBinding
	CASBindings []ColumnBinding
	// Conditions is used by WHERE to locate primary key
	Conditions []Condition
	// IfConditions is used by IF which is put after WHERE to restrict non primary key columns
	IfConditions   []Condition
	ResultBindings map[string]ColumnBinding
	// Debug flag will cause all CQL statements to get logged
	Debug       bool
	ReadOptions *ReadOptions
	// Setting Keyspace to a non-zero value will cause CQL statements to be qualified by this keyspace.
	Keyspace string
	// Setting StaticKeyspace to true will cause the generated CQL to be qualified by the keyspace the code was generated against.
	StaticKeyspace bool
	logger         Logger
}

type Logger interface {
	Printf(format string, args ...interface{})
}

func defaultReadOptions() *ReadOptions {
	return &ReadOptions{Distinct: false}
}

// NewContext creates a fresh Context instance using standard logger and logs to stderr with cqlc prefix
// If you want statement debugging, set the Debug property to true
func NewContext() *Context {
	stdLogger := log.New(os.Stderr, "cqlc: ", log.Lshortfile|log.Ldate)
	return NewContextWithLogger(stdLogger)
}

// NewContextWithLogger creates a fresh Context with custom logger
func NewContextWithLogger(logger Logger) *Context {
	return &Context{Debug: false, ReadOptions: defaultReadOptions(), logger: logger}
}

// NewDebugContext creates a fresh Context with debug turned on
func NewDebugContext() *Context {
	c := NewContext()
	c.Debug = true
	return c
}

type Executable interface {
	Exec(*gocql.Session) error
	Batch(*gocql.Batch) error
}

type CompareAndSwap interface {
	Swap(*gocql.Session) (bool, error)
}

type Fetchable interface {
	Bindable
	// Limit constrains the number of rows returned by a query
	Limit(limit int) Fetchable
	Prepare(session *gocql.Session) (*gocql.Query, error)
	Fetch(*gocql.Session) (*gocql.Iter, error)
}

type UniqueFetchable interface {
	// Returns true if the statement did return a result, false if it did not.
	FetchOne(*gocql.Session) (bool, error)
}

type IfQueryStep interface {
	If(conditions ...Condition) Query
	Query
}

type Query interface {
	Executable
	Fetchable
	// OrderBy sets the ordering of the returned query
	OrderBy(col ...ClusteredColumn) Fetchable
}

type SelectWhereStep interface {
	Fetchable
	Where(conditions ...Condition) IfQueryStep
}

type SelectFromStep interface {
	From(table Table) SelectWhereStep
}

type SelectSelectStep interface {
	Select(cols ...Column) SelectFromStep
	// Builds a SELECT DISTINCT statement in CQL - this operation can only be used
	// with a partitioned column.
	SelectDistinct(col PartitionedColumn) SelectFromStep
}

type IncrementWhereStep interface {
	Having(conditions ...Condition) Executable
}

type IncrementCounterStep interface {
	IncrementWhereStep
	Increment(col CounterColumn, value int64) IncrementCounterStep
}

type Upsertable interface {
	Table
	SupportsUpsert() bool
}

type CounterTable interface {
	Table
	IsCounterTable() bool
}

type Table interface {
	TableName() string
	Keyspace() string
	ColumnDefinitions() []Column
}

type Column interface {
	ColumnName() string
}

// ListColumn is a marker interface to denote that column maps to CQL list type
type ListColumn interface {
	Column
	ListType() Column
}

// PartitionedColumn is a marker interface to denote that a column is partitioned.
type PartitionedColumn interface {
	// Returns the column definition that a column family is partitioned by.
	PartitionBy() Column
}

// ClusteredColumn is a marker interface to denote that a column is clustered.
type ClusteredColumn interface {
	// Returns the column name that a column family is clustered with.
	ClusterWith() string
	// This specifies descending ORDER BY for the column.
	// If this method is not called, it an ascending order will be assumed.
	Desc() ClusteredColumn
	// Denotes whether a descending order should be applied
	IsDescending() bool
}

type Bindable interface {
	Bind(cols ...ColumnBinding) UniqueFetchable
	// Into sets the target binding to fetch the result of a single row query into
	Into(TableBinding) UniqueFetchable
}

type Condition struct {
	Binding   ColumnBinding
	Predicate PredicateType
}

type ColumnBinding struct {
	Column Column
	Value  interface{}
	// If Incremental is true, this column should be interpreted as an incremental operation on a collection column
	Incremental             bool
	CollectionType          CollectionType
	CollectionOperationType CollectionOperationType
}

// KeyValue is used for bind map value by key
type KeyValue struct {
	Key   interface{}
	Value interface{}
}

type TableBinding struct {
	Table   Table
	Columns []ColumnBinding
}

type BindingError string

func (m BindingError) Error() string {
	return string(m)
}

func (c *Context) Select(cols ...Column) SelectFromStep {
	c.Columns = cols
	c.Operation = ReadOperation
	return c
}

func (c *Context) SelectDistinct(col PartitionedColumn) SelectFromStep {
	c.Columns = []Column{col.PartitionBy()}
	c.Operation = ReadOperation
	c.ReadOptions.Distinct = true
	return c
}

func (c *Context) Limit(lim int) Fetchable {
	c.ReadOptions.Limit = lim
	return c
}

func (c *Context) OrderBy(cols ...ClusteredColumn) Fetchable {
	spec := make([]OrderSpec, len(cols))
	for i, c := range cols {
		spec[i] = OrderSpec{Col: c.ClusterWith(), Desc: c.IsDescending()}
	}

	c.ReadOptions.Ordering = spec
	return c
}

func (c *Context) From(t Table) SelectWhereStep {
	c.Table = t
	//c.logger = c.logger.WithField("table", t.TableName())
	return c
}

func (c *Context) Delete(cols ...Column) SelectFromStep {
	c.Columns = cols
	c.Operation = DeleteOperation
	return c
}

func (c *Context) UpdateCounter(t CounterTable) IncrementCounterStep {
	c.Table = t
	c.Operation = CounterOperation
	return c
}

func (c *Context) Increment(col CounterColumn, value int64) IncrementCounterStep {
	set(c, col, value)
	return c
}

func (c *Context) Having(cond ...Condition) Executable {
	c.Conditions = cond
	return c
}

func (c *Context) Upsert(u Upsertable) SetValueStep {
	c.Table = u
	c.Operation = WriteOperation
	//c.logger = c.logger.WithField("table", u.TableName())
	return c
}

// Convenience method to generate CQL counter updates.
// This generates CQL SET clauses for each of the counter columns
// and CQL WHERE clauses for each of the key columns.
func (c *Context) Add(b TableBinding) Executable {
	c.Table = b.Table
	c.Operation = CounterOperation

	bindings := make([]ColumnBinding, 0)
	conds := make([]Condition, 0)

	for _, binding := range b.Columns {
		_, ok := binding.Column.(CounterColumn)
		if ok {
			bindings = append(bindings, binding)
		} else {
			cond := Condition{Binding: binding, Predicate: EqPredicate}
			conds = append(conds, cond)
		}
	}

	c.Bindings = bindings
	c.Conditions = conds

	return c
}

func (c *Context) Store(b TableBinding) Executable {
	c.Table = b.Table
	c.Operation = WriteOperation
	c.Bindings = b.Columns
	return c
}

func (c *Context) Into(b TableBinding) UniqueFetchable {
	c.Table = b.Table
	c.Operation = ReadOperation
	c.Bind(b.Columns...)
	return c
}

// Adds each column binding as a `SET col = ?` fragment in the resulting CQL
func (c *Context) Apply(cols ...ColumnBinding) SetValueStep {
	for _, col := range cols {
		// TODO Can't we just append the whole list? Or just set it explicitly?
		set(c, col.Column, col.Value)
	}
	return c
}

// Adds column bindings whose values will be populated if a CAS operation
// is applied.
func (c *Context) IfExists(cols ...ColumnBinding) CompareAndSwap {
	c.CASBindings = cols
	return c
}

func (c *Context) If(cond ...Condition) Query {
	c.IfConditions = cond
	return c
}

func (c *Context) Where(cond ...Condition) IfQueryStep {
	c.Conditions = cond
	return c
}

func (c *Context) Bind(cols ...ColumnBinding) UniqueFetchable {
	c.ResultBindings = make(map[string]ColumnBinding)

	for _, col := range cols {
		c.ResultBindings[col.Column.ColumnName()] = col
	}

	return c
}

func (c *Context) FetchOne(s *gocql.Session) (bool, error) {

	iter, err := c.Fetch(s)
	if err != nil {
		return false, err
	}

	cols := iter.Columns()
	row := make([]interface{}, len(cols))

	for i := 0; i < len(cols); i++ {

		name := cols[i].Name
		binding, ok := c.ResultBindings[name]

		if !ok {
			row[i] = cols[i].TypeInfo.New()
			if c.Debug && row[i] == nil {
				log.Printf("Could not map type info: %+v", cols[i].TypeInfo.Type())
			}
		} else {
			row[i] = binding.Value
		}
	}

	found := iter.Scan(row...)

	err = iter.Close()
	if err != nil {
		return found, err
	}

	return found, nil
}

func (c *Context) Fetch(s *gocql.Session) (*gocql.Iter, error) {
	q, err := c.Prepare(s)
	if err != nil {
		return nil, err
	}
	return q.Iter(), nil
}

// Prepare is used in Select, it only has where condition binding
// Prepare is only called by Fetch
func (c *Context) Prepare(s *gocql.Session) (*gocql.Query, error) {

	stmt, err := c.RenderCQL()
	if err != nil {
		return nil, err
	}

	// The reason why this is so dynamic is because of WHERE foo IN (?,?,?) clauses
	// The factoring for an IN clause is bad, since we are storing an array into the value
	// and using reflection to dig it out again
	// This should be more strongly typed

	placeHolders := make([]interface{}, 0)

	for _, cond := range c.Conditions {
		v := cond.Binding.Value

		switch reflect.TypeOf(v).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(v)
			for i := 0; i < s.Len(); i++ {
				placeHolders = append(placeHolders, s.Index(i).Interface())
			}
		case reflect.Array:

			// Not really happy about having to special case UUIDs
			// but this works for now

			if val, ok := v.(gocql.UUID); ok {
				placeHolders = append(placeHolders, val.Bytes())
			} else {
				return nil, bindingErrorf("Cannot bind component: %+v (type: %s)", v, reflect.TypeOf(v))
			}
		default:
			// TODO: (pingginp) why it took address of interface and it worked ...
			//placeHolders = append(placeHolders, &v)
			placeHolders = append(placeHolders, v)
		}
	}

	c.Dispose()

	if c.Debug {
		debugStmt(c.logger, stmt, placeHolders)
	}

	return s.Query(stmt, placeHolders...), nil
}

func (c *Context) Exec(s *gocql.Session) error {
	stmt, placeHolders, err := BuildStatement(c)

	if err != nil {
		return err
	}

	if c.Debug {
		debugStmt(c.logger, stmt, placeHolders)
	}

	return s.Query(stmt, placeHolders...).Exec()
}

// Returns true if the CAS operation was applied, false otherwise.
// If the operation was applied, then the result bindings will not be popluated.
func (c *Context) Swap(s *gocql.Session) (bool, error) {

	if len(c.CASBindings) == 0 {
		return false, ErrCASBindings
	}

	casPlaceHolders := make([]interface{}, len(c.CASBindings))
	for i, binding := range c.CASBindings {
		casPlaceHolders[i] = binding.Value
	}

	stmt, queryPlaceholders, err := BuildStatement(c)
	if err != nil {
		return false, err
	}

	return s.Query(stmt, queryPlaceholders...).ScanCAS(casPlaceHolders...)
}

func (c *Context) Batch(b *gocql.Batch) error {
	stmt, placeHolders, err := BuildStatement(c)

	if err != nil {
		return err
	}

	if c.Debug {
		debugStmt(c.logger, stmt, placeHolders)
	}

	b.Query(stmt, placeHolders...)

	return nil
}

func debugStmt(logger Logger, stmt string, placeHolders []interface{}) {
	infused := strings.Replace(stmt, "?", " %+v", -1)
	var buffer bytes.Buffer
	buffer.WriteString("CQL: ")
	buffer.WriteString(infused)
	logger.Printf(buffer.String(), placeHolders...)
	//panic("debugStmt")
}

// BuildStatement is the new BuildStatement based on Prepare to support set map value by key
// BuildStatement is used in update, thus it has binding and where condition binding
// BuildStatement is called by Exec, Batch, Swap
func BuildStatement(c *Context) (stmt string, placeHolders []interface{}, err error) {
	stmt, err = c.RenderCQL()
	if err != nil {
		return "", nil, errors.Wrap(err, "error render CQL")
	}

	// placeHolders are the bindings that will be passed to gocql
	placeHolders = make([]interface{}, 0)

	// NOTE: for all binding we need to expand value due to multiple placeholders in one binding
	// in bindings we have foo[?] = ?
	// in where bindings we have where foo in (?, ?, ?)

	for _, bind := range c.Bindings {
		v := bind.Value
		switch bind.CollectionType {
		case MapType:
			switch bind.CollectionOperationType {
			case SetByKey:
				kv, ok := v.(KeyValue)
				if !ok {
					return "", nil, errors.Errorf("map set by key requires KeyValue binding on column %s", bind.Column.ColumnName())
				}
				placeHolders = append(placeHolders, kv.Key, kv.Value)
			}
		default:
			placeHolders = append(placeHolders, v)
		}
	}

	bindCondition := func(conditions []Condition) error {
		for _, cond := range conditions {
			v := cond.Binding.Value
			switch reflect.TypeOf(v).Kind() {
			case reflect.Slice:
				s := reflect.ValueOf(v)
				for i := 0; i < s.Len(); i++ {
					placeHolders = append(placeHolders, s.Index(i).Interface())
				}
			case reflect.Array:

				// Not really happy about having to special case UUIDs
				// but this works for now

				if val, ok := v.(gocql.UUID); ok {
					placeHolders = append(placeHolders, val.Bytes())
				} else {
					return bindingErrorf("Cannot bind component: %+v (type: %s)", v, reflect.TypeOf(v))
				}
			default:
				placeHolders = append(placeHolders, v)
			}
		}
		return nil
	}

	if err := bindCondition(c.Conditions); err != nil {
		return "", nil, err
	}
	if err := bindCondition(c.IfConditions); err != nil {
		return "", nil, err
	}

	c.Dispose()

	return stmt, placeHolders, nil
}

// TODO Make this private, since we should be able to test against BuildStatement()
func (c *Context) RenderCQL() (string, error) {

	var buf bytes.Buffer

	switch c.Operation {
	case ReadOperation:
		{
			renderSelect(c, &buf)
		}
	case WriteOperation:
		{
			if c.hasConditions() {
				renderUpdate(c, &buf, false)
			} else {
				renderInsert(c, &buf)
			}

			renderCAS(c, &buf)
		}
	case CounterOperation:
		{
			renderUpdate(c, &buf, true)
		}
	case DeleteOperation:
		{
			renderDelete(c, &buf)
		}
	default:
		return "", fmt.Errorf("Unknown operation type: %v", c.Operation)
	}

	return buf.String(), nil
}

func (c *Context) Dispose() {
	c.Columns = nil
	c.Operation = None
	c.Table = nil
	c.Bindings = nil
	c.Conditions = nil
	c.IfConditions = nil
	c.CASBindings = nil
}

func Truncate(s *gocql.Session, t Table) error {
	stmt := fmt.Sprintf("TRUNCATE %s", t.TableName())
	return s.Query(stmt).Exec()
}

func set(c *Context, col Column, value interface{}) {
	c.Bindings = append(c.Bindings, ColumnBinding{Column: col, Value: value})
}

func setMap(c *Context, col Column, key interface{}, value interface{}) {
	b := ColumnBinding{Column: col, Value: KeyValue{Key: key, Value: value}, CollectionType: MapType, CollectionOperationType: SetByKey}
	c.Bindings = append(c.Bindings, b)
}

func appendList(c *Context, col ListColumn, values interface{}) {
	b := ColumnBinding{Column: col, Value: values, Incremental: true, CollectionType: ListType, CollectionOperationType: Append}
	c.Bindings = append(c.Bindings, b)
}

func prependList(c *Context, col ListColumn, values interface{}) {
	b := ColumnBinding{Column: col, Value: values, Incremental: true, CollectionType: ListType, CollectionOperationType: Prepend}
	c.Bindings = append(c.Bindings, b)
}

func removeList(c *Context, col ListColumn, values interface{}) {
	b := ColumnBinding{Column: col, Value: values, Incremental: true, CollectionType: ListType, CollectionOperationType: RemoveByValue}
	c.Bindings = append(c.Bindings, b)
}

func (c *Context) hasConditions() bool {
	return len(c.Conditions) > 0
}

func bindingErrorf(format string, args ...interface{}) BindingError {
	return BindingError(fmt.Sprintf(format, args...))
}
