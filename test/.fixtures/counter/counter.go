package main

import (
    "github.com/relops/cqlc/cqlc"
    "github.com/relops/cqlc/integration"
    "log"
    "os"
)

var COUNTER = BasicCounterTableDef()

func main() {
    session := integration.TestSession("127.0.0.1", "cqlc")
    integration.Truncate(session, COUNTER)

    result := "FAILED"

    ctx := cqlc.NewContext()

    err := ctx.UpdateCounter(COUNTER).
        Increment(COUNTER.COUNTER_COLUMN, 13).
        Having(COUNTER.ID.Eq("x")).
        Exec(session)

    if err != nil {
        log.Fatalf("Could not execute counter increment: %v", err)
    }

    iter, err := ctx.Select(COUNTER.COUNTER_COLUMN).
        From(COUNTER).
        Where(COUNTER.ID.Eq("x")).
        Fetch(session)

    counters := BindBasicCounter(iter)

    err = iter.Close()
    if err != nil {
        log.Fatalf("Could not bind data: %v", err)
        return
    }

    if len(counters) == 1 {
        if counters[0].CounterColumn == 13 {
            result = "PASSED"
        }
    }

    os.Stdout.WriteString(result)
}
