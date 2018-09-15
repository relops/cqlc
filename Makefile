CCM_NODE ?= node1
CQLSH_CMD ?= ccm $(CCM_NODE) cqlsh

.PHONY: build
build:
	go build -o build/cqlc .

.PHONY: install
install:
	go install .

test/collections.cql: test/tmpl/schema.tmpl test/schema_generator.go
	cd test; go run schema_generator.go

test/.fixtures/collections/input.go: test/tmpl/input.tmpl test/schema_generator.go
	cd test; go run schema_generator.go

schema: test/collections.cql
	-$(CQLSH_CMD) -f test/keyspace.cql
	$(CQLSH_CMD) -k cqlc -f test/schema.cql
	$(CQLSH_CMD) -k cqlc -f test/collections.cql
	$(CQLSH_CMD) -k cqlc -f test/shared.cql
	$(CQLSH_CMD) -k cqlc2 -f test/shared.cql

cqlc/columns.go: cqlc/tmpl/columns.tmpl cqlc/column_generator.go
	cd cqlc; go run column_generator.go

columns: cqlc/columns.go

input: test/.fixtures/collections/input.go test/collections.cql

test: columns schema test/.fixtures/collections/input.go
	go test -v ./...

format:
	gofmt -w cqlc generator integration test

.PHONY: test columns bindata