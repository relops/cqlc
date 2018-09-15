# cqlc

[![Build Status](https://travis-ci.org/pingginp/cqlc.svg?branch=master)](https://travis-ci.org/pingginp/cqlc)

This a fork of [relops/cqlc](https://github.com/relops/cqlc) which is no longer maintained

## Usage

````bash
make install
cqlc --instance=127.0.0.1 --keyspace=cqlc --package=foo --output=foo.go --symbols
````

## Dev

````bash
# generate columns
make cqlc/columns.go
````

````go
func (c *Context) Prepare(s *gocql.Session) (*gocql.Query, error) {
	stmt, err := c.RenderCQL()
}

// cqlc.go
func (c *Context) RenderCQL() (string, error) {
	switch c.Operation {
	case ReadOperation:
		renderSelect(c, &buf)
	case WriteOperation:
		if c.hasConditions() {
			renderUpdate(c, &buf, false)
		} else {
			renderInsert(c, &buf)
		}
		renderCAS(c, &buf)
	case CounterOperation:
		renderUpdate(c, &buf, true)
	case DeleteOperation:
		renderDelete(c, &buf)
	default:
		return "", fmt.Errorf("Unknown operation type: %v", c.Operation)
	}
}

// render.go
func renderUpdate(ctx *Context, buf *bytes.Buffer, counterTable bool) {
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
				}
			default:
				setFragments[i] = fmt.Sprintf("%s = ?", col)
			}
		}
	}
}
````