// +build ignore

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"text/template"
)

type TypeInfo struct {
	Pre string
	Cql string
	Lit string
	Ex  string
}

var types = []TypeInfo{
	TypeInfo{Pre: "String", Cql: "text", Lit: "string", Ex: "\"x\""},
	TypeInfo{Pre: "Int32", Cql: "int", Lit: "int32", Ex: "1"},
	TypeInfo{Pre: "Int64", Cql: "bigint", Lit: "int64", Ex: "1"},
	TypeInfo{Pre: "Float32", Cql: "float", Lit: "float32", Ex: "1.1"},
	TypeInfo{Pre: "Float64", Cql: "double", Lit: "float64", Ex: "1.1"},
	TypeInfo{Pre: "Timestamp", Cql: "timestamp", Lit: "time.Time", Ex: "time.Now().UTC().Truncate(time.Millisecond)"},
	TypeInfo{Pre: "Timeuuid", Cql: "timeuuid", Lit: "gocql.UUID", Ex: "gocql.TimeUUID()"},
	TypeInfo{Pre: "Uuid", Cql: "uuid", Lit: "gocql.UUID", Ex: "gocql.TimeUUID()"},
	TypeInfo{Pre: "Boolean", Cql: "boolean", Lit: "bool", Ex: "true"},
	TypeInfo{Pre: "Decimal", Cql: "decimal", Lit: "*inf.Dec", Ex: "inf.NewDec(1,1)"},
	TypeInfo{Pre: "Bytes", Cql: "blob", Lit: "[]byte", Ex: "[]byte(\"x\")"},
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

	if err := ioutil.WriteFile("collections.cql", b.Bytes(), 0644); err != nil {
		log.Errorf("Could not write templated file: %s", err)
		return
	}

	log.Info("Regenerated test schema")

	t, err = template.New("input.tmpl").ParseFiles("tmpl/input.tmpl")
	if err != nil {
		log.Errorf("Could not open template: %s", err)
		return
	}

	b.Reset()
	t.Execute(&b, params)

	if err := ioutil.WriteFile(".fixtures/collections/input.go", b.Bytes(), 0644); err != nil {
		log.Errorf("Could not write templated file: %s", err)
		return
	}

	log.Info("Regenerated test input data")
}
