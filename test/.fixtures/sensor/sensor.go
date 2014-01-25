package main

import (
    "os"
    "log"
    "github.com/relops/cqlc/cqlc"
    "github.com/relops/cqlc/integration"
    "tux21b.org/v1/gocql/uuid"
)

var EVENTS = EventsTableDef()

func main() {

    session := integration.TestSession("127.0.0.1", "cqlc")
    integration.Truncate(session, EVENTS)

    result := "FAILED"

    ctx := cqlc.NewContext()

    var sensorId int64 = 100

    ctx.Upsert(EVENTS).
    	SetInt64(EVENTS.SENSOR, sensorId).
    	SetTimeUUID(EVENTS.TIMESTAMP, uuid.TimeUUID()).
    	SetFloat32(EVENTS.TEMPERATURE, 19.8).
    	SetInt32(EVENTS.PRESSURE, 357).
    	Exec(session)

    iter, err := ctx.Select().
    				From(EVENTS).
    				Where(
    					EVENTS.SENSOR.Eq(sensorId),
    					EVENTS.TIMESTAMP.Lt(uuid.TimeUUID()) ).
    				Fetch(session)

    if err != nil {
        log.Fatalf("Could not execute query: %v", err)
        return
    }

    var events []Events = BindEvents(iter)
    
    err = iter.Close()
    if err != nil {
        log.Fatalf("Could not bind data: %v", err)
        return
    }
	
    if len(events) == 1 {
    	result = "PASSED"	
    }

    os.Stdout.WriteString(result)
}