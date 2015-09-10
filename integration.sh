#!/bin/bash

set -e

function run_tests() {
	local clusterSize=1
	local version=$1

	local conf=(
	    "concurrent_reads: 2"
	    "concurrent_writes: 2"
	    "rpc_server_type: sync"
	    "rpc_min_threads: 2"
	    "rpc_max_threads: 2"
	    "write_request_timeout_in_ms: 5000"
	    "read_request_timeout_in_ms: 5000"
	)

	ccm remove test || true

	ccm create test -v binary:$version -n $clusterSize -d --vnodes --jvm_arg="-Xmx256m -XX:NewSize=100m"
    ccm updateconf "${conf[@]}"

	ccm start -v
	ccm status
	ccm node1 nodetool status

	make test

	ccm remove
}

run_tests $1
