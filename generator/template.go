package generator

import (
	"bytes"
	"fmt"
	"github.com/gocql/gocql"
	"regexp"
	"strings"
	"text/template"
)

var bindingTemplate *template.Template
var camelRegex = regexp.MustCompile("[0-9A-Za-z]+")

func init() {
	m := template.FuncMap{
		"toUpper":               strings.ToUpper,
		"sprint":                fmt.Sprint,
		"snakeToCamel":          snakeToCamel,
		"columnType":            columnType,
		"valueType":             valueType,
		"isCounterColumn":       isCounterColumn,
		"supportsClustering":    supportsClustering,
		"supportsPartitioning":  supportsPartitioning,
		"isListType":            isListType,
		"hasSecondaryIndex":     hasSecondaryIndex,
		"isLastComponent":       isLastComponent,
		"isCounterColumnFamily": isCounterColumnFamily,
	}
	temp, _ := generator_tmpl_binding_tmpl()
	bindingTemplate = template.Must(template.New("binding.tmpl").Funcs(m).Parse(string(temp)))
}

// TODO This is metadata specific to the column family that should be cached at compilation
// rather than being post-processed like this
func isCounterColumnFamily(t gocql.TableMetadata) bool {
	for _, col := range t.Columns {
		if isCounterColumn(*col) {
			return true
		}
	}
	return false
}

func isCounterColumn(c gocql.ColumnMetadata) bool {
	return c.Type.Type() == gocql.TypeCounter
}

func supportsClustering(c gocql.ColumnMetadata) bool {
	return c.Kind == gocql.ColumnClusteringKey
}

func supportsPartitioning(c gocql.ColumnMetadata) bool {
	return c.Kind == gocql.ColumnPartitionKey
}

func isLastComponent(c gocql.ColumnMetadata, t *gocql.TableMetadata) bool {
	switch c.Kind {
	case gocql.ColumnPartitionKey:
		lastPartitionKeyColumn := t.PartitionKey[len(t.PartitionKey)-1]
		return c.Name == lastPartitionKeyColumn.Name
	case gocql.ColumnClusteringKey:
		lastClusteringColumn := t.ClusteringColumns[len(t.ClusteringColumns)-1]
		return c.Name == lastClusteringColumn.Name
	default:
		return false
	}
}

func isListType(c gocql.ColumnMetadata) bool {
	return c.Type.Type() == gocql.TypeList || c.Type.Type() == gocql.TypeSet
}

func hasSecondaryIndex(c gocql.ColumnMetadata) bool {
	return c.Index.Name != ""
}

func columnType(c gocql.ColumnMetadata, table *gocql.TableMetadata) string {

	t := c.Type

	baseType := columnTypes[t.Type()]

	// TODO The Kind field should be an enum, not a string
	if c.Kind == gocql.ColumnClusteringKey {
		replacement := ".Clustered"
		if isLastComponent(c, table) {
			replacement = ".LastClustered"
		}
		baseType = strings.Replace(baseType, ".", replacement, 1)
	} else if c.Kind == gocql.ColumnPartitionKey {
		replacement := ".Partitioned"
		if isLastComponent(c, table) {
			replacement = ".LastPartitioned"
		}
		baseType = strings.Replace(baseType, ".", replacement, 1)
	} else if c.Index.Name != "" {
		replacement := ".Equality"
		baseType = strings.Replace(baseType, ".", replacement, 1)
	}

	switch t.Type() {
	case gocql.TypeMap:
		// TODO This is very hacky - basically the types need to to be strings
		// in order to template out properly
		// Resolving these to integer enums is not helpful, as this example shows

		// HACK UPDATE (04/03/2015): The domain and range types have been
		// superseded by gocql.TypeInfo.{Key,Elem}, but this still needs to get pulled through

		// TODO should probably not swallow this
		ct, _ := t.(gocql.CollectionType)

		key := columnTypes[ct.Key.Type()]
		elem := columnTypes[ct.Elem.Type()]

		key = strings.Replace(key, "_Column", "", 1)

		elem = strings.Replace(elem, "_", "Map", 1)
		elem = strings.Replace(elem, "cqlc.", "", 1)

		return fmt.Sprintf("%s%s", key, elem)
	case gocql.TypeList, gocql.TypeSet:
		// TODO should probably not swallow this
		ct, _ := t.(gocql.CollectionType)
		elem := columnTypes[ct.Elem.Type()]
		return strings.Replace(elem, "_", "Slice", 1)
	default:
		return strings.Replace(baseType, "_", "", 1)
	}

	return baseType
}

func valueType(c gocql.ColumnMetadata) string {

	t := c.Type

	switch t.Type() {
	case gocql.TypeList, gocql.TypeSet:
		// TODO should probably not swallow this
		ct, _ := t.(gocql.CollectionType)
		literal := literalTypes[ct.Elem.Type()]
		return fmt.Sprintf("[]%s", literal)
	case gocql.TypeMap:
		// TODO should probably not swallow this
		ct, _ := t.(gocql.CollectionType)
		key := literalTypes[ct.Key.Type()]
		elem := literalTypes[ct.Elem.Type()]
		return fmt.Sprintf("map[%s]%s", key, elem)
	default:
		return literalTypes[t.Type()]
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
