DROP KEYSPACE cqlc;
CREATE KEYSPACE cqlc WITH replication = 
{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };