package generator

import (
	"bytes"
	"fmt"
	"github.com/relops/cqlc/meta"
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
	return c.DataInfo.DomainType == meta.CounterType
}

func columnType(c Column) string {
	baseType := columnTypes[c.DataInfo.DomainType]
	if c.SupportsClustering() {
		replacement := ".Clustered"
		if c.IsLastComponent {
			replacement = ".LastClustered"
		}
		baseType = strings.Replace(baseType, ".", replacement, 1)
	} else if c.SupportsPartitioning() {
		replacement := ".Partitioned"
		if c.IsLastComponent {
			replacement = ".LastPartitioned"
		}
		baseType = strings.Replace(baseType, ".", replacement, 1)
	} else if c.SecondaryIndex {
		replacement := ".Equality"
		baseType = strings.Replace(baseType, ".", replacement, 1)
	}

	switch c.DataInfo.GenericType {
	case meta.SliceType:
		{
			//fmt.Printf("Column: %+v (%s)\n", c, baseType)
			baseType = strings.Replace(baseType, "_", "Slice", 1)
		}
	case meta.MapType:
		{
			//fmt.Printf("Column: %+v (%s)\n", c, baseType)
			// TODO This is very hacky - basically the types need to to be strings
			// in order to template out properly
			// Resolving these to integer enums is not helpful, as this example shows
			rangeType := columnTypes[c.DataInfo.RangeType]
			rangeType = strings.Replace(rangeType, "_", "Map", 1)
			rangeType = strings.Replace(rangeType, "cqlc.", "", 1)
			baseType = strings.Replace(baseType, "_Column", "", 1)
			baseType = fmt.Sprintf("%s%s", baseType, rangeType)
		}
	default:
		{
			baseType = strings.Replace(baseType, "_", "", 1)
		}
	}

	return baseType
}

func valueType(c Column) string {
	domain := literalTypes[c.DataInfo.DomainType]

	switch c.DataInfo.GenericType {
	case meta.SliceType:
		{
			return fmt.Sprintf("[]%s", domain)
		}
	case meta.MapType:
		{
			rangeType := literalTypes[c.DataInfo.RangeType]
			return fmt.Sprintf("map[%s]%s", domain, rangeType)
		}
	default:
		{
			return domain
		}
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
