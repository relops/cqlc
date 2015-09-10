// +build ignore

package main

import (
	"bytes"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
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
	Prefix  string
	Literal string
}

var types = []TypeInfo{
	TypeInfo{Prefix: "String", Literal: "string"},
	TypeInfo{Prefix: "Int32", Literal: "int32"},
	TypeInfo{Prefix: "Int64", Literal: "int64"},
	TypeInfo{Prefix: "Float32", Literal: "float32"},
	TypeInfo{Prefix: "Float64", Literal: "float64"},
	TypeInfo{Prefix: "Timestamp", Literal: "time.Time"},
	TypeInfo{Prefix: "TimeUUID", Literal: "gocql.UUID"},
	TypeInfo{Prefix: "UUID", Literal: "gocql.UUID"},
	TypeInfo{Prefix: "Boolean", Literal: "bool"},
	TypeInfo{Prefix: "Decimal", Literal: "*inf.Dec"},
	TypeInfo{Prefix: "Varint", Literal: "*big.Int"},
	TypeInfo{Prefix: "Bytes", Literal: "[]byte"},
}

func main() {
	params := make(map[string]interface{})
	params["types"] = types

	t, err := template.New("columns.tmpl").ParseFiles("tmpl/columns.tmpl")
	if err != nil {
		log.Errorf("Could not open template: %s", err)
		return
	}

	var b bytes.Buffer
	t.Execute(&b, params)

	if err := ioutil.WriteFile("columns.go", b.Bytes(), 0644); err != nil {
		log.Errorf("Could not write templated file: %s", err)
		return
	}

	log.Info("Regenerated columns")
}
