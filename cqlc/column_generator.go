// +build ignore

package main

import (
	"bytes"
	"go/format"
	"io/ioutil"
	"log"
	"text/template"
)

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
		log.Fatalf("Could not open template: %s", err)
		return
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, params); err != nil {
		log.Fatalf("Could not render template: %s", err)
		return
	}

	b, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatalf("Could not format rendered template as go code: %s", err)
		return
	}

	if err := ioutil.WriteFile("columns.go", b, 0644); err != nil {
		log.Fatalf("Could not write templated file: %s", err)
		return
	}

	log.Println("Regenerated columns")
}
