# Set map value by key instead of entire map

This doc describes https://github.com/pingginp/cqlc/issues/2

- in `tmpl/columns.tmpl` 
  - in `SetStepValue` interface, add `Set{{Type1}}{{Type2}}MapValue(col, key {{type1}}, value {{type2}}` 
  - in `Context` methods, add the impl method with same name and call `setMap`
- run `make cqlc/columns.go` to generate using `column_generator.go`
- add `setMap` and use `[]interface{}{key, value}` as `Value` for binding, set type to MapType and op to `SetByKey`
- in `renderUpdate`, use `%s[?] = ?` where `%s` is from col
- `BuildStatement`, flatten binding, (copy from Prepare), former is used by Exec (update & insert) and latter is used by Fetch (select)
  - NOTE: we added new struct `KeyValue` to avoid flatten slice value that should be kept as slice
  - [ ] in old code, they were using `&v` for placeholder, it's pointer to interface, I don't know why it didn't trigger error, because gocql traverse ptr until it's a value?

````go
// KeyValue is used for bind map value by key
type KeyValue struct {
	Key   interface{}
	Value interface{}
}


// Prepare is used in Select, it only has where condition binding
// Prepare is only called by Fetch
func (c *Context) Prepare(s *gocql.Session) (*gocql.Query, error) {
	stmt, err := c.RenderCQL()
	
	// nothing is changed for update map
}

// BuildStatement is the new BuildStatement based on Prepare to support set map value by key
// BuildStatement is used in update, thus it has binding and where condition binding
// BuildStatement is called by Exec, Batch, Swap
func BuildStatement(c *Context) (stmt string, placeHolders []interface{}, err error) {
	stmt, err = c.RenderCQL()

	// placeHolders are the bindings that will be passed to gocql
	placeHolders = make([]interface{}, 0)

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