test/collections.cql: test/tmpl/schema.tmpl test/schema_generator.go
	cd test; go run schema_generator.go

test/.fixtures/collections/input.go: test/tmpl/input.tmpl test/schema_generator.go
	cd test; go run schema_generator.go

schema: test/collections.cql
	cqlsh -f test/keyspace.cql
	cqlsh -k cqlc -f test/schema.cql
	cqlsh -k cqlc -f test/collections.cql
	cqlsh -k cqlc -f test/shared.cql
	cqlsh -k cqlc2 -f test/shared.cql

cqlc/columns.go: cqlc/tmpl/columns.tmpl cqlc/column_generator.go
	cd cqlc; go run column_generator.go

generator/validator_parser.go: generator/tmpl/validator_parser.tmpl
	cd generator; rm validator_parser.go; go run validator_parser_generator.go

columns: cqlc/columns.go 

bindata: generator/binding_tmpl.go

validator: generator/validator_parser.go

input: test/.fixtures/collections/input.go test/collections.cql

generator/binding_tmpl.go: generator/tmpl/binding.tmpl
	go-bindata -pkg=generator -o=generator/binding_tmpl.go generator/tmpl

test: columns bindata schema validator test/.fixtures/collections/input.go
	go test -v ./...

format:
	gofmt -w cqlc generator integration test

.PHONY: test columns bindata