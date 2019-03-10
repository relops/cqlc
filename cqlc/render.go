package cqlc

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

var predicateTypes = map[PredicateType]string{
	EqPredicate: "=",
	GtPredicate: ">",
	GePredicate: ">=",
	LtPredicate: "<",
	LePredicate: "<=",
}

func renderSelect(ctx *Context, buf *bytes.Buffer) {

	if ctx.ReadOptions.Distinct {
		fmt.Fprint(buf, "SELECT DISTINCT ")
	} else {
		fmt.Fprint(buf, "SELECT ")
	}

	var colClause string
	if len(ctx.Columns) == 0 {
		colClause = columnClause(ctx.Table.ColumnDefinitions())

	} else {
		colClause = columnClause(ctx.Columns)
	}

	fmt.Fprint(buf, colClause)

	if ctx.Keyspace == "" && !ctx.StaticKeyspace {
		fmt.Fprintf(buf, " FROM %s", ctx.Table.TableName())
	} else if ctx.StaticKeyspace {
		fmt.Fprintf(buf, " FROM %s.%s", ctx.Table.Keyspace(), ctx.Table.TableName())
	} else {
		fmt.Fprintf(buf, " FROM %s.%s", ctx.Keyspace, ctx.Table.TableName())
	}

	if ctx.hasConditions() {
		fmt.Fprint(buf, " ")
		renderWhereClause(ctx, buf)
	}

	if len(ctx.ReadOptions.Ordering) > 0 {

		orderByFragments := make([]string, len(ctx.ReadOptions.Ordering))

		for i, order := range ctx.ReadOptions.Ordering {
			if order.Desc {
				orderByFragments[i] = fmt.Sprintf("%s DESC", order.Col)
			} else {
				orderByFragments[i] = order.Col
			}
		}

		orderBy := strings.Join(orderByFragments, ", ")
		fmt.Fprintf(buf, " ORDER BY %s", orderBy)
	}

	if ctx.ReadOptions.Limit > 0 {
		fmt.Fprintf(buf, " LIMIT %d", ctx.ReadOptions.Limit)
	}
}

func columnClause(cols []Column) string {
	colFragments := make([]string, len(cols))
	for i, col := range cols {
		colFragments[i] = col.ColumnName()
	}
	return strings.Join(colFragments, ", ")
}

func renderInsert(ctx *Context, buf *bytes.Buffer) {

	if ctx.Keyspace == "" && !ctx.StaticKeyspace {
		fmt.Fprintf(buf, "INSERT INTO %s (", ctx.Table.TableName())
	} else if ctx.StaticKeyspace {
		fmt.Fprintf(buf, "INSERT INTO %s.%s (", ctx.Table.Keyspace(), ctx.Table.TableName())
	} else {
		fmt.Fprintf(buf, "INSERT INTO %s.%s (", ctx.Keyspace, ctx.Table.TableName())
	}

	colFragments := make([]string, len(ctx.Bindings))
	for i, binding := range ctx.Bindings {
		col := binding.Column.ColumnName()
		colFragments[i] = col
	}
	colClause := strings.Join(colFragments, ", ")
	fmt.Fprint(buf, colClause)

	fmt.Fprint(buf, ") VALUES (")

	placeHolderFragments := make([]string, len(ctx.Bindings))
	for i, _ := range ctx.Bindings {
		placeHolderFragments[i] = "?"
	}

	placeHolderClause := strings.Join(placeHolderFragments, ",")
	fmt.Fprint(buf, placeHolderClause)
	fmt.Fprint(buf, ")")

}

func renderUpdate(ctx *Context, buf *bytes.Buffer, counterTable bool) {

	if ctx.Keyspace == "" && !ctx.StaticKeyspace {
		fmt.Fprintf(buf, "UPDATE %s SET ", ctx.Table.TableName())
	} else if ctx.StaticKeyspace {
		fmt.Fprintf(buf, "UPDATE %s.%s SET ", ctx.Table.Keyspace(), ctx.Table.TableName())
	} else {
		fmt.Fprintf(buf, "UPDATE %s.%s SET ", ctx.Keyspace, ctx.Table.TableName())
	}

	setFragments := make([]string, len(ctx.Bindings))
	for i, binding := range ctx.Bindings {
		col := binding.Column.ColumnName()

		if counterTable {
			setFragments[i] = fmt.Sprintf("%s = %s + ?", col, col)
		} else {

			switch binding.CollectionType {
			case ListType:
				switch binding.CollectionOperationType {
				case Append:
					setFragments[i] = fmt.Sprintf("%s = %s + ?", col, col)
				case Prepend:
					setFragments[i] = fmt.Sprintf("%s = ? + %s", col, col)
				case RemoveByValue:
					setFragments[i] = fmt.Sprintf("%s = %s - ?", col, col)
				}
			case MapType:
				switch binding.CollectionOperationType {
				case SetByKey:
					setFragments[i] = fmt.Sprintf("%s[?] = ?", col)
					// TODO: it seems in C*2 and C*3 the + has different semantic
					// https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_map_t.html
					// https://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertMap.html
					//case Append:
					//	setFragments[i] = fmt.Sprintf("%s = %s + ?", col, col)
					//case RemoveByKey:
					//	setFragments[i] = fmt.Sprintf("%s = %s - ?", col, col)
				default:
					panic(fmt.Sprintf("unsupported Map CollectionOperationType %d", binding.CollectionOperationType))
				}
			default:
				setFragments[i] = fmt.Sprintf("%s = ?", col)
			}
		}
	}

	setClause := strings.Join(setFragments, ", ")
	fmt.Fprint(buf, setClause)

	fmt.Fprint(buf, " ")
	renderWhereClause(ctx, buf)
}

func renderCAS(ctx *Context, buf *bytes.Buffer) {
	if len(ctx.CASBindings) > 0 {
		fmt.Fprint(buf, " IF NOT EXISTS")
	}
}

func renderDelete(ctx *Context, buf *bytes.Buffer) {
	fmt.Fprint(buf, "DELETE ")

	if len(ctx.Columns) > 0 {
		fmt.Fprint(buf, columnClause(ctx.Columns))
		fmt.Fprint(buf, " ")
	}

	if ctx.Keyspace == "" && !ctx.StaticKeyspace {
		fmt.Fprintf(buf, "FROM %s ", ctx.Table.TableName())
	} else if ctx.StaticKeyspace {
		fmt.Fprintf(buf, "FROM %s.%s ", ctx.Table.Keyspace(), ctx.Table.TableName())
	} else {
		fmt.Fprintf(buf, "FROM %s.%s ", ctx.Keyspace, ctx.Table.TableName())
	}

	renderWhereClause(ctx, buf)

	if len(ctx.IfConditions) > 0 {
		renderIfClause(ctx, buf)
	}
}

func renderWhereClause(ctx *Context, buf *bytes.Buffer) {
	fmt.Fprint(buf, "WHERE ")
	renderCondition(buf, ctx.Conditions)
}

func renderIfClause(ctx *Context, buf *bytes.Buffer) {
	fmt.Fprint(buf, " IF ")
	renderCondition(buf, ctx.IfConditions)
}

func renderCondition(buf *bytes.Buffer, conditions []Condition) {
	whereFragments := make([]string, len(conditions))
	for i, condition := range conditions {
		col := condition.Binding.Column.ColumnName()

		pred := condition.Predicate

		if pred == InPredicate {

			predValues := reflect.ValueOf(condition.Binding.Value)
			placeHolders := make([]string, predValues.Len())
			for i := 0; i < predValues.Len(); i++ {
				placeHolders[i] = "?"
			}
			valueString := strings.Join(placeHolders, ",")
			whereFragments[i] = fmt.Sprintf("%s IN (%s)", col, valueString)

		} else {
			whereFragments[i] = fmt.Sprintf("%s %s ?", col, predicateTypes[pred])
		}
	}

	whereClause := strings.Join(whereFragments, " AND ")
	fmt.Fprint(buf, whereClause)
}
