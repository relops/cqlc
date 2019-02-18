# cqlc

[![Build Status](https://travis-ci.org/pingginp/cqlc.svg?branch=master)](https://travis-ci.org/pingginp/cqlc)

This a fork of [relops/cqlc](https://github.com/relops/cqlc) the upstream is no longer maintained.

## Usage

````bash
# install the generator to $GOPATH/bin
make install
# generate table and column definition based on schema in keyspace cqlc
cqlc --instance=127.0.0.1 --keyspace=cqlc --package=foo --output=foo.go --symbols
````

You need to change the repo path in `glide.yaml` to use this fork

````yaml
- package: github.com/relops/cqlc
  version: master
  repo: https://github.com/pingginp/cqlc.git
````

## Dev

- clone the repo to `$GOPATH/src/github.com/relops/cqlc`

````bash
# generate columns
make cqlc/columns.go
# e2e test
# TODO: you need to run it twice if schema changed because first time it will generate package based on schema, which won't get compiled ...
# this same as when using latex ... you do xelatex several times when there is bib ...
make travis-test
make travis-tear
# release, update cqlc/ver.go, build and zip binary for all three platforms, only mac is tested
make release
````

The code has two part, runtime and generator

- [cqlc](cqlc) is the runtime, a query builder, don't get mislead by the [column_generator.go](cqlc/column_generator.go)
it is mainly for generating runtime code that ships with the library
- [generator](generator) generates table and column definition based on schema, NOTE: it does [NOT support Cassandra 3](https://github.com/pingginp/cqlc/issues/7)

### Runtime

The main modification we have are listed below
 
- [support update map value by key](doc/set-map-value-by-key.md), previously, cqlc can only update entire map. (This change only requires update runtime)

### Generator

- generator now compiles, caused by breaking change of constant name in gocql

The overall generator logic is

- get table meta using gocql
- render the template defined in `tmpl.go` using template helper methods defined in `template.go`
  - `valueType` is returning empty value for `text`, just add a new mapping in `literalTypes` fixed this [#7](https://github.com/pingginp/cqlc/issues/7)