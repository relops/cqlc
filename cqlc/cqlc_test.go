package cqlc

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type MockTable struct {
	name    string
	columns []Column
}

type MockAsciiColumn struct {
	name string
}

type MockInt32Column struct {
	name string
}

type MockCounterColumn struct {
	name string
}

func (t *MockTable) TableName() string {
	return t.name
}

func (t *MockTable) SupportsUpsert() bool {
	return true
}

func (t *MockTable) IsCounterTable() bool {
	return true
}

func (t *MockTable) ColumnDefinitions() []Column {
	return t.columns
}

func (t *MockAsciiColumn) ColumnName() string {
	return t.name
}

func (t *MockAsciiColumn) Supports(value string) bool {
	return true
}

func (t *MockAsciiColumn) Eq(value string) Condition {
	return eq(t, value)
}

func (t *MockAsciiColumn) In(value ...string) Condition {
	condition := Condition{
		Binding:   ColumnBinding{Column: t, Value: value},
		Predicate: InPredicate,
	}
	return condition
}

func (t *MockAsciiColumn) Ge(value string) Condition {
	return ge(t, value)
}

func (t *MockAsciiColumn) Gt(value string) Condition {
	return gt(t, value)
}

func (t *MockAsciiColumn) Le(value string) Condition {
	return le(t, value)
}

func (t *MockAsciiColumn) Lt(value string) Condition {
	return lt(t, value)
}

func (t *MockInt32Column) ColumnName() string {
	return t.name
}

func (t *MockInt32Column) Supports(value int32) bool {
	return true
}

func (t *MockInt32Column) Eq(value int32) Condition {
	return eq(t, value)
}

func (t *MockCounterColumn) ColumnName() string {
	return t.name
}

func (t *MockCounterColumn) CanIncrement() bool {
	return true
}

func eq(c Column, value interface{}) Condition {
	return mockCondition(c, value, EqPredicate)
}

func gt(c Column, value interface{}) Condition {
	return mockCondition(c, value, GtPredicate)
}

func ge(c Column, value interface{}) Condition {
	return mockCondition(c, value, GePredicate)
}

func lt(c Column, value interface{}) Condition {
	return mockCondition(c, value, LtPredicate)
}

func le(c Column, value interface{}) Condition {
	return mockCondition(c, value, LePredicate)
}

func mockCondition(c Column, value interface{}, pred PredicateType) Condition {
	condition := Condition{
		Binding:   ColumnBinding{Column: c, Value: value},
		Predicate: pred,
	}
	return condition
}

type CqlTestSuite struct {
	suite.Suite
	table *MockTable
}

func (s *CqlTestSuite) SetupTest() {
	s.table = &MockTable{}
	s.table.name = "foo"
	s.table.columns = []Column{
		&MockAsciiColumn{name: "bar"},
	}
}

func TestCqlSuite(t *testing.T) {
	suite.Run(t, new(CqlTestSuite))
}

func (s *CqlTestSuite) TestSelect() {
	idCol := &MockAsciiColumn{name: "id"}
	barCol := &MockAsciiColumn{name: "bar"}
	c := NewContext()

	/*
		c.Select(barCol).From(s.table)
		cql, err := c.RenderCQL()
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), cql, "SELECT bar FROM foo")
	*/

	c.Select(barCol).From(s.table).Where(idCol.Eq("x"))
	cql, err := c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "SELECT bar FROM foo WHERE id = ?")

	c.Select(barCol).From(s.table).Where(idCol.Gt("x"))
	cql, err = c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "SELECT bar FROM foo WHERE id > ?")

	c.Select(barCol).From(s.table).Where(idCol.Ge("x"))
	cql, err = c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "SELECT bar FROM foo WHERE id >= ?")

	c.Select(barCol).From(s.table).Where(idCol.Lt("x"))
	cql, err = c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "SELECT bar FROM foo WHERE id < ?")

	c.Select(barCol).From(s.table).Where(idCol.Le("x"))
	cql, err = c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "SELECT bar FROM foo WHERE id <= ?")

	c.Select(barCol).From(s.table).Where(idCol.In("x", "y", "z"))
	cql, err = c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "SELECT bar FROM foo WHERE id IN (?,?,?)")
}

func (s *CqlTestSuite) TestInsert() {
	barCol := &MockAsciiColumn{name: "bar"}
	quuxCol := &MockInt32Column{name: "quux"}
	c := NewContext()
	c.Upsert(s.table).SetString(barCol, "baz").SetInt32(quuxCol, 10)
	cql, err := c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "INSERT INTO foo (bar, quux) VALUES (?,?)")
}

func (s *CqlTestSuite) TestUpdate() {
	idCol := &MockAsciiColumn{name: "id"}
	barCol := &MockAsciiColumn{name: "bar"}
	quuxCol := &MockInt32Column{name: "quux"}
	c := NewContext()
	c.Upsert(s.table).SetString(barCol, "baz").SetInt32(quuxCol, 10).Where(idCol.Eq("x"))
	cql, err := c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "UPDATE foo SET bar = ?, quux = ? WHERE id = ?")
}

func (s *CqlTestSuite) TestCounter() {
	idCol := &MockAsciiColumn{name: "id"}
	cntCol := &MockCounterColumn{name: "cnt"}
	c := NewContext()
	c.UpdateCounter(s.table).Increment(cntCol, int64(100)).Having(idCol.Eq("x"))
	cql, err := c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "UPDATE foo SET cnt = cnt + ? WHERE id = ?")
}

func (s *CqlTestSuite) TestDeleteRow() {
	idCol := &MockAsciiColumn{name: "id"}
	c := NewContext()
	c.Delete().From(s.table).Where(idCol.Eq("x"))
	cql, err := c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "DELETE FROM foo WHERE id = ?")
}

func (s *CqlTestSuite) TestDeleteColumn() {
	idCol := &MockAsciiColumn{name: "id"}
	barCol := &MockAsciiColumn{name: "bar"}
	c := NewContext()
	c.Delete(barCol).From(s.table).Where(idCol.Eq("x"))
	cql, err := c.RenderCQL()
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cql, "DELETE bar FROM foo WHERE id = ?")
}
