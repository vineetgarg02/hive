-- create table
 -- numeric type
 set hive.stats.autogather=false;
 set hive.support.concurrency=true;
 set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;



CREATE TABLE table1(i int CHECK -i > -10,
    j int CHECK +j > 10,
    ij boolean CHECK ij IS NOT NULL,
    a int CHECK a BETWEEN i AND j,
    bb float CHECK bb IN (23.4,56,4),
    d bigint CHECK d > round(567.6) AND d < round(1000.4))
    clustered by (i) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');
DESC FORMATTED table1;

EXPLAIN INSERT INTO table1 values(1,100,true, 5, 23.4, 700.5);
INSERT INTO table1 values(1,100,true, 5, 23.4, 700.5);
SELECT * from table1;
DROP TABLE table1;


-- null check constraint
CREATE TABLE table2(i int CHECK i + NULL > 0);
DESC FORMATTED table2;
EXPLAIN INSERT INTO table2 values(8);
INSERT INTO table2 values(8);
select * from table2;
Drop table table2;

-- UDF created by users
CREATE FUNCTION test_udf2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';
CREATE TABLE tudf(v string CHECK test_udf2(v) <> 'vin');
EXPLAIN INSERT INTO tudf values('function1');
Drop table tudf;

-- multiple constraints
create table tmulti(url string NOT NULL ENABLE, userName string, numClicks int CHECK numClicks > 0, d date);
alter table tmulti add constraint un1 UNIQUE (userName, numClicks) DISABLE;
DESC formatted tmulti;
EXPLAIN INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
INSERT INTO tmulti value('hive.apache.com', 'user1', 48, '12-01-2018');
Select * from tmulti;
Drop table tmulti;

-- case insentivity
create table tcase(url string NOT NULL ENABLE, userName string, d date, numClicks int CHECK numclicks > 0);
DESC formatted tmulti;
EXPLAIN INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
INSERT INTO tmulti value('hive.apache.com', 'user1', 48, '12-01-2018');
Select * from tmulti;
Drop table tmulti;


