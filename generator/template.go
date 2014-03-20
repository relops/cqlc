package generator

import (
	"bytes"
	"fmt"
	"regexp"
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

func isCounterColumn(c Column) bool {
	return c.DataType == CounterType
}

func columnType(c Column) string {
	baseType := columnTypes[c.DataType]
	if c.SupportsClustering() {
		replacement := ".Clustered"
		if c.IsLastComponent {
			replacement = ".LastClustered"
		}
		return strings.Replace(baseType, ".", replacement, 1)
	} else if c.SupportsPartitioning() {
		replacement := ".Partitioned"
		if c.IsLastComponent {
			replacement = ".LastPartitioned"
		}
		return strings.Replace(baseType, ".", replacement, 1)
	} else if c.SecondaryIndex {
		replacement := ".Equality"
		return strings.Replace(baseType, ".", replacement, 1)
	} else {
		return baseType
	}
}

func valueType(c Column) string {
	return literalTypes[c.DataType]
}

func snakeToCamel(src string) string {
	byteSrc := []byte(src)
	chunks := camelRegex.FindAll(byteSrc, -1)
	for i, val := range chunks {
		chunks[i] = bytes.Title(val)
	}
	return string(bytes.Join(chunks, nil))
}
