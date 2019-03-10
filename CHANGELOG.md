# Changelog

NOTE: this file format is based on [gaocegege/maintainer](https://github.com/gaocegege/maintainer/blob/master/CHANGELOG.md)

## Unreleased

## 0.13.0 (2019-03-10)

Was going to make it 0.12.2 but since it breaks both runtime and generated code, bump minor version number

- support `IF` in `DELETE` [#13](https://github.com/pingginp/cqlc/issues/13)
- in generated column bindings allow `Eq` on all columns, previously only primary key, index are allowed, which blocks using 
other columns in condition queries after `If`

## 0.12.1 (2019-02-18)

- previous release didn't update all the mapping in generator

## 0.12.0 (2019-02-18)

[Closed issues](https://github.com/pingginp/cqlc/issues?q=is%3Aissue+milestone%3A0.12.0+is%3Aclosed)

Merged pull requests

- [#12](https://github.com/pingginp/cqlc/pull/12) one line fix to support Cassandra 3

## 0.11.0 (2018-09-15)

[Closed issues](https://github.com/pingginp/cqlc/issues?q=is%3Aissue+is%3Aclosed+milestone%3A0.11.0)
 
Merged pull requests

- Reboot [#4](https://github.com/pingginp/cqlc/pull/4) the project now compiles and support set map value by key
- [#9](https://github.com/pingginp/cqlc/pull/9) remove `log.Fatal` and use logrus

## 0.10.5 (2015-09-10)

The last commit in upstream https://github.com/relops/cqlc/commit/9427a2081fb4f4910b0af8fc80d09c109b4f9815