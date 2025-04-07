CREATE KEYSPACE test_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE test_keyspace.prepared_statement_test (
    a int PRIMARY KEY,
    b int,
    c int
);

use test_keyspace;

insert into test_keyspace.prepared_statement_test(a,b,c) values (1,1,1);
insert into test_keyspace.prepared_statement_test(a,b,c) values (2,2,2);
insert into test_keyspace.prepared_statement_test(a,b,c) values (3,3,3);