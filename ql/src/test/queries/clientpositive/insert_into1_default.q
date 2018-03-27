set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
-- SORT_QUERY_RESULTS

DROP TABLE insert_into1;

-- No default constraint
CREATE TABLE insert_into1 (key int, value string)
    clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

EXPLAIN INSERT INTO TABLE insert_into1 values(default, DEFAULT);
INSERT INTO TABLE insert_into1 values(default, DEFAULT);
SELECT * from insert_into1;
TRUNCATE table insert_into1;

-- should be able to use any case for DEFAULT
EXPLAIN INSERT INTO TABLE insert_into1 values(234, dEfAULt);
INSERT INTO TABLE insert_into1 values(234, dEfAULt);
SELECT * from insert_into1;
TRUNCATE table insert_into1;

-- multi values
explain insert into insert_into1 values(default, 3),(2,default);
insert into insert_into1 values(default, 3),(2,default);
select * from insert_into1;
TRUNCATE table insert_into1;

--with column schema
EXPLAIN INSERT INTO TABLE insert_into1(key) values(default);
INSERT INTO TABLE insert_into1(key) values(default);
select * from insert_into1;
TRUNCATE table insert_into1;

EXPLAIN INSERT INTO TABLE insert_into1(key, value) values(2,default);
INSERT INTO TABLE insert_into1(key, value) values(2,default);
select * from insert_into1;
TRUNCATE table insert_into1;

DROP TABLE insert_into1;

-- with default constraint
CREATE TABLE insert_into1 (key int DEFAULT 1, value string)
    clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
EXPLAIN INSERT INTO TABLE insert_into1 values(default, DEFAULT);
INSERT INTO TABLE insert_into1 values(default, DEFAULT);
SELECT * from insert_into1;
TRUNCATE table insert_into1;

-- should be able to use any case for DEFAULT
EXPLAIN INSERT INTO TABLE insert_into1 values(234, dEfAULt);
INSERT INTO TABLE insert_into1 values(234, dEfAULt);
SELECT * from insert_into1;
TRUNCATE table insert_into1;

-- multi values
explain insert into insert_into1 values(default, 3),(2,default);
insert into insert_into1 values(default, 3),(2,default);
select * from insert_into1;
TRUNCATE table insert_into1;

--with column schema
EXPLAIN INSERT INTO TABLE insert_into1(key) values(default);
INSERT INTO TABLE insert_into1(key) values(default);
select * from insert_into1;
TRUNCATE table insert_into1;

EXPLAIN INSERT INTO TABLE insert_into1(key, value) values(2,default);
INSERT INTO TABLE insert_into1(key, value) values(2,default);
select * from insert_into1;
TRUNCATE table insert_into1;

EXPLAIN INSERT INTO TABLE insert_into1(key, value) values(2,default),(DEFAULT, default);
INSERT INTO TABLE insert_into1(key, value) values(2,default),(DEFAULT, default);
select * from insert_into1;
TRUNCATE table insert_into1;
DROP TABLE insert_into1;


-- UPDATE
CREATE TABLE insert_into1 (key int DEFAULT 1, value string, i int)
    clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

INSERT INTO insert_into1 values(2,1, 45);
EXPLAIN UPDATE insert_into1 set key = DEFAULT where value=1;
UPDATE insert_into1 set key = DEFAULT where value=1;
SELECT * from insert_into1;
TRUNCATE table insert_into1;

INSERT INTO insert_into1 values(2,1, 45);
EXPLAIN UPDATE insert_into1 set key = DEFAULT, value=DEFAULT where value=1;
UPDATE insert_into1 set key = DEFAULT, value=DEFAULT where value=1;
SELECT * from insert_into1;
TRUNCATE table insert_into1;

DROP TABLE insert_into1;
