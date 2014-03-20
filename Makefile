schema:
	cqlsh -f test/keyspace.cql
	cqlsh -k cqlc -f test/schema.cql

cqlc/standard_columns.go: cqlc/column_generator.go
	 cd cqlc; go run column_generator.go

columns: cqlc/standard_columns.go 

bindata: generator/binding_tmpl.go

generator/binding_tmpl.go: generator/tmpl/binding.tmpl
	go-bindata -pkg=generator -o=generator/binding_tmpl.go generator/tmpl

test: columns bindata schema
	go test -v ./...

format:
	gofmt -w cqlc generator integration test

.PHONY: test columns bindata