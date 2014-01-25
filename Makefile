bindata: generator/binding_tmpl.go

generator/%_tmpl.go: generator/tmpl/%.tmpl
	cat $< | go-bindata -func=$*_tmpl -pkg=generator | gofmt > $@

test: bindata
	go test -v ./...

format:
	gofmt --tabs=false --tabwidth=4 -w cql generator integration test

.PHONY: test