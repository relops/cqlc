// +build ignore

package main

import (
	"bytes"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"os"
	"text/template"
)

var logConfig = `
<seelog type="sync">
	<outputs formatid="main">
		<console/>
	</outputs>
	<formats>
		<format id="main" format="%Date(2006-02-01 03:04:05.000) - %Msg%n"/>
	</formats>
</seelog>`

func init() {
	logger, err := log.LoggerFromConfigAsString(logConfig)

	if err != nil {
		fmt.Printf("Could not load seelog configuration: %s\n", err)
		return
	}

	log.ReplaceLogger(logger)
}

type TypeInfo struct {
	Prefix string
	Cql    string
}

var types = []TypeInfo{
	TypeInfo{Prefix: "String", Cql: "text"},
	TypeInfo{Prefix: "Int32", Cql: "int"},
	TypeInfo{Prefix: "Int64", Cql: "bigint"},
	TypeInfo{Prefix: "Float32", Cql: "float"},
	TypeInfo{Prefix: "Float64", Cql: "double"},
	TypeInfo{Prefix: "Timestamp", Cql: "timestamp"},
	TypeInfo{Prefix: "TimeUUID", Cql: "timeuuid"},
	TypeInfo{Prefix: "UUID", Cql: "uuid"},
	TypeInfo{Prefix: "Boolean", Cql: "boolean"},
	TypeInfo{Prefix: "Decimal", Cql: "decimal"},
	TypeInfo{Prefix: "Bytes", Cql: "blob"},
}

func main() {
	params := make(map[string]interface{})
	params["types"] = types

	t, err := template.New("schema.tmpl").ParseFiles("tmpl/schema.tmpl")
	if err != nil {
		log.Errorf("Could not open template: %s", err)
		return
	}

	var b bytes.Buffer
	t.Execute(&b, params)

	if err := ioutil.WriteFile("collections.cql", b.Bytes(), os.ModePerm); err != nil {
		log.Errorf("Could not write templated file: %s", err)
		return
	}

	log.Info("Regenerated test schema")
}
