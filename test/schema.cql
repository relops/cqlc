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
    timeuuid_column timeuuid,
    map_column map<text, text>,
    array_column list<text>,
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
