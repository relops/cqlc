schema:
	cqlsh -f test/keyspace.cql
	cqlsh -k cqlc -f test/schema.cql

bindata: generator/binding_tmpl.go

generator/%_tmpl.go: generator/tmpl/%.tmpl
	cat $< | go-bindata -func=$*_tmpl -pkg=generator | gofmt > $@

test: bindata schema
	go test -v ./...

format:
	gofmt -w cqlc generator integration test

.PHONY: test