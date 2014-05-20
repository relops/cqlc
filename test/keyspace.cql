DROP KEYSPACE cqlc;
CREATE KEYSPACE cqlc WITH replication = 
{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

DROP KEYSPACE cqlc2;
CREATE KEYSPACE cqlc2 WITH replication = 
{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };