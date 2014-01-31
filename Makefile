schema:
	cqlsh -f test/keyspace.cql
	cqlsh -k cqlc -f test/schema.cql

bindata: generator/binding_tmpl.go

generator/binding_tmpl.go: generator/tmpl/binding.tmpl
	go-bindata -pkg=generator -o=generator/binding_tmpl.go generator/tmpl

test: bindata schema
	go test -v ./...

format:
	gofmt -w cqlc generator integration test

.PHONY: test