CREATE TABLE basic
(
    id ascii,
    ascii_column ascii,
    int32_column int,
    int64_column bigint,
    timestamp_column timestamp,
    boolean_column boolean,
    varchar_column varchar,
    text_column varchar,
    float_column float,
    double_column double,
    decimal_column decimal,
    varint_column varint,
    timeuuid_column timeuuid,
    uuid_column uuid,
    map_column map<text, text>,
    array_column list<text>,
    set_column set<text>,
    PRIMARY KEY (id)
);

CREATE TABLE really_basic
(
    id ascii,
    int32_column int,
    PRIMARY KEY (id)
);

CREATE TABLE basic_clustered
(
    id bigint,
    int32_cluster int,
    int32_column int,
    PRIMARY KEY (id, int32_cluster)
);

CREATE TABLE basic_counter
(
    id ascii,
    counter_column counter,
    PRIMARY KEY (id)
);

CREATE TABLE cluster_by_string_and_int
(
    id ascii,
    string_cluster ascii,
    int64_cluster bigint,
    last_cluster_element bigint,
    int32_column int,
    PRIMARY KEY (id, string_cluster, int64_cluster, last_cluster_element)
);

CREATE TABLE basic_blob
(
    id ascii,
    blob_column blob,
    PRIMARY KEY (id)
);

CREATE TABLE first_timeline
(
    when timeuuid,
    tag ascii,
    PRIMARY KEY (when)
);

CREATE TABLE second_timeline
(
    when timeuuid,
    latitude float,
    longitude float,
    PRIMARY KEY (when)
);

-- Verify that reverse cluster ordering works as expected

create table reverse_timeseries (
  event_type text,
  insertion_time timestamp,
  event blob,
  PRIMARY KEY (event_type, insertion_time)
)
WITH CLUSTERING ORDER BY (insertion_time DESC);

-- https://github.com/relops/cqlc/issues/7

CREATE TABLE simple_indexed_composite (
   x int,
   y int,
   z int,
   PRIMARY KEY ((x,y),z)
);

CREATE INDEX simple_indexed_composite_by_y
ON simple_indexed_composite(y);

-- https://github.com/relops/cqlc/issues/17

CREATE TABLE indexed_map (
   x int,
   y map<varchar,varchar>,
   PRIMARY KEY (x)
);

CREATE INDEX indexed_map_by_y
ON indexed_map(KEYS(y));

-- Pair of orginal and a clone

CREATE TABLE original
(
    id ascii,    
    date timestamp,
    comment text,
    PRIMARY KEY (id, date)
);

CREATE TABLE clone
(
    id ascii,
    date timestamp,
    comment text,
    PRIMARY KEY (id, date)
);

CREATE TABLE events
(
    sensor bigint,
    timestamp timeuuid,
    temperature float,
    pressure int,
    PRIMARY KEY (sensor, timestamp)
);

CREATE TABLE significant_events
(
    sensor bigint,
    timestamp timeuuid,
    significance int,
    temperature float,
    pressure int,
    PRIMARY KEY (sensor, timestamp, significance)
);

-- Examples

CREATE TABLE user_accounts (
    username text,
    email text,
    password text,
    last_visited timestamp,
    country text,
    PRIMARY KEY (username)
);

CREATE INDEX user_accounts_country ON user_accounts(country);
