package generator

import (
	"bytes"
	"fmt"
	"github.com/gocql/gocql"
	"regexp"
	"sort"
	"strings"
	"text/template"
)

var bindingTemplate *template.Template
var camelRegex = regexp.MustCompile("[0-9A-Za-z]+")

func init() {
	m := template.FuncMap{
		"toUpper":         strings.ToUpper,
		"sprint":          fmt.Sprint,
		"snakeToCamel":    snakeToCamel,
		"columnType":      columnType,
		"valueType":       valueType,
		"isCounterColumn": isCounterColumn,
	}
	temp, _ := generator_tmpl_binding_tmpl()
	bindingTemplate = template.Must(template.New("binding.tmpl").Funcs(m).Parse(string(temp)))
}

// ###########################################################
// TODO Delete this expensive hack

type ByComponentIndexHack []*gocql.ColumnMetadata

func (a ByComponentIndexHack) Len() int           { return len(a) }
func (a ByComponentIndexHack) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByComponentIndexHack) Less(i, j int) bool { return a[i].ComponentIndex < a[j].ComponentIndex }

// ###########################################################

func isCounterColumn(c gocql.ColumnMetadata) bool {
	return c.Type.Type == gocql.TypeCounter
}

func columnType(c gocql.ColumnMetadata, allCols map[string]*gocql.ColumnMetadata) string {

	// ###########################################################
	cols := make([]*gocql.ColumnMetadata, 0, len(allCols))

	for _, meta := range allCols {
		cols = append(cols, meta)
	}

	sort.Sort(sort.Reverse(ByComponentIndexHack(cols)))

	isLastComponent := false
	foundParitioned := false
	foundClustered := false

	for i, _ := range cols {

		if foundClustered && foundParitioned {
			break
		}

		if !foundClustered {
			if cols[i].Kind == gocql.CLUSTERING_KEY {
				isLastComponent = true
				foundClustered = true
			}
		}

		if !foundParitioned {
			if cols[i].Kind == gocql.PARTITION_KEY {
				isLastComponent = true
				foundParitioned = true
			}
		}
	}

	// ###########################################################

	t := c.Type

	baseType := columnTypes[t.Type]

	// TODO The Kind field should be an enum, not a string
	if c.Kind == gocql.CLUSTERING_KEY {
		replacement := ".Clustered"
		if isLastComponent {
			replacement = ".LastClustered"
		}
		baseType = strings.Replace(baseType, ".", replacement, 1)
	} else if c.Kind == gocql.PARTITION_KEY {
		replacement := ".Partitioned"
		if isLastComponent {
			replacement = ".LastPartitioned"
		}
		baseType = strings.Replace(baseType, ".", replacement, 1)
	} else if c.Index.Name != "" {
		replacement := ".Equality"
		baseType = strings.Replace(baseType, ".", replacement, 1)
	}

	switch t.Type {
	case gocql.TypeMap:
		// TODO This is very hacky - basically the types need to to be strings
		// in order to template out properly
		// Resolving these to integer enums is not helpful, as this example shows

		// HACK UPDATE (04/03/2015): The domain and range types have been
		// superseded by gocql.TypeInfo.{Key,Elem}, but this still needs to get pulled through

		key := columnTypes[t.Key.Type]
		elem := columnTypes[t.Elem.Type]

		key = strings.Replace(key, "_Column", "", 1)

		elem = strings.Replace(elem, "_", "Map", 1)
		elem = strings.Replace(elem, "cqlc.", "", 1)

		return fmt.Sprintf("%s%s", key, elem)
	case gocql.TypeList, gocql.TypeSet:
		elem := columnTypes[t.Elem.Type]
		return strings.Replace(elem, "_", "Slice", 1)
	default:
		return strings.Replace(baseType, "_", "", 1)
	}

	return baseType
}

func valueType(c gocql.ColumnMetadata) string {

	t := c.Type

	switch t.Type {
	case gocql.TypeList, gocql.TypeSet:
		literal := literalTypes[t.Elem.Type]
		return fmt.Sprintf("[]%s", literal)
	case gocql.TypeMap:
		key := literalTypes[t.Key.Type]
		elem := literalTypes[t.Elem.Type]
		return fmt.Sprintf("map[%s]%s", key, elem)
	default:
		return literalTypes[t.Type]
	}

}

func snakeToCamel(src string) string {
	byteSrc := []byte(src)
	chunks := camelRegex.FindAll(byteSrc, -1)
	for i, val := range chunks {
		chunks[i] = bytes.Title(val)
	}
	return string(bytes.Join(chunks, nil))
}
