# cqlc

[![Build Status](https://travis-ci.org/pingginp/cqlc.svg?branch=master)](https://travis-ci.org/pingginp/cqlc)

This a fork of [relops/cqlc](https://github.com/relops/cqlc) which is no longer maintained

## Usage

````bash
make install
cqlc --instance=127.0.0.1 --keyspace=cqlc --package=foo --output=foo.go --symbols
````

You need to change the repo path in `glide.yaml` to use this fork

````yaml
- package: github.com/relops/cqlc
  version: master
  repo: https://github.com/pingginp/cqlc.git
````

## Dev

- clone the repo to `$GOPATH/src/github.com/relops/cqlc`

````bash
# generate columns
make cqlc/columns.go
# e2e test
# TODO: you need to run it twice if schema changed because first time it will dump the schema, which won't get compiled ...
# this same as when using latex ... you do xelatex several times when there is bib ...
make travis-test
make travis-tear
````

````go
// NOTE: in order to support set map by value, we must flatten binding,
// previously it is only did in Prepare and ignored in BuildStatement

// Prepare is used in Select, it only has where condition binding
// Prepare is only called by Fetch
func (c *Context) Prepare(s *gocql.Session) (*gocql.Query, error) {
	stmt, err := c.RenderCQL()
}

// BuildStatement is used in update, thus it has binding and where condition binding
// BuildStatement is called by Exec, Batch, Swap
func BuildStatement(c *Context) (stmt string, placeHolders []interface{}, err error) {
	
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