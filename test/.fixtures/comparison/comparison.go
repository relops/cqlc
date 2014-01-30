package main

import (
	"github.com/relops/cqlc/cqlc"
	"github.com/relops/cqlc/integration"
	"github.com/tux21b/gocql"
	"log"
	"os"
)

var CLUSTER_BY_STRING_AND_INT = ClusterByStringAndIntTableDef()

func main() {
	session := integration.TestSession("127.0.0.1", "cqlc")
	integration.Truncate(session, CLUSTER_BY_STRING_AND_INT)

	result := "FAILED"

	ctx := cqlc.NewContext()

	err := ctx.Upsert(CLUSTER_BY_STRING_AND_INT).
		SetString(CLUSTER_BY_STRING_AND_INT.ID, "a").
		SetString(CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER, "y").
		SetInt64(CLUSTER_BY_STRING_AND_INT.INT64_CLUSTER, 4).
		SetInt64(CLUSTER_BY_STRING_AND_INT.LAST_CLUSTER_ELEMENT, 40).
		SetInt32(CLUSTER_BY_STRING_AND_INT.INT32_COLUMN, 100).
		Exec(session)

	if err != nil {
		log.Fatalf("Could not execute upsert: %v", err)
		os.Exit(1)
	}

	id := CLUSTER_BY_STRING_AND_INT.ID.Eq("a")

	passed := expectComparisonResult(ctx, session, expect(1), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Gt("x"))
	passed = expectComparisonResult(ctx, session, expect(0), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Gt("y"))
	passed = expectComparisonResult(ctx, session, expect(0), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Ge("z"))
	passed = expectComparisonResult(ctx, session, expect(1), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Lt("z"))
	passed = expectComparisonResult(ctx, session, expect(1), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Le("y"))
	passed = expectComparisonResult(ctx, session, expect(0), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Lt("y"))
	passed = expectComparisonResult(ctx, session, expect(0), id, CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Lt("y"))

	stringCluster := CLUSTER_BY_STRING_AND_INT.STRING_CLUSTER.Eq("y")
	int64Cluster := CLUSTER_BY_STRING_AND_INT.INT64_CLUSTER.Eq(4)
	lastClusterElement := CLUSTER_BY_STRING_AND_INT.LAST_CLUSTER_ELEMENT.In(39, 40, 41)

	passed = expectComparisonResult(ctx, session, expect(1), id, stringCluster, int64Cluster, lastClusterElement)

	if passed {
		result = "PASSED"
	}

	os.Stdout.WriteString(result)
}

func expectComparisonResult(ctx *cqlc.Context,
	s *gocql.Session,
	callback func([]ClusterByStringAndInt) bool,
	comparisons ...cqlc.Condition) bool {

	iter, err := ctx.Select().
		From(CLUSTER_BY_STRING_AND_INT).
		Where(comparisons...).
		Fetch(s)

	if err != nil {
		log.Fatalf("Could not run query: %v", err)
		os.Exit(1)
	}

	clustered := BindClusterByStringAndInt(iter)

	err = iter.Close()
	if err != nil {
		log.Fatalf("Could not bind data: %v", err)
		os.Exit(1)
	}

	return callback(clustered)
}

func expect(n int) func([]ClusterByStringAndInt) bool {
	return func(c []ClusterByStringAndInt) bool {
		return len(c) == n
	}
}
