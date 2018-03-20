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
INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
Select * from tmulti;
Drop table tmulti;

-- case insentivity
create table tcase(url string NOT NULL ENABLE, userName string, d date, numClicks int CHECK numclicks > 0);
DESC formatted tcase;
EXPLAIN INSERT INTO tcase values('hive.apache.com', 'user1', '12-01-2018', 48);
INSERT INTO tcase values('hive.apache.com', 'user1', '12-01-2018', 48);
Select * from tcase ;
Drop table tcase;

-- cast
create table tcast(url string NOT NULL ENABLE, numClicks int,
    price FLOAT CHECK cast(numClicks as FLOAT)*price > 10.00);
DESC FORMATTED tcast;
EXPLAIN INSERT INTO tcast values('www.google.com', 100, cast(0.5 as float));
INSERT INTO tcast values('www.google.com', 100, cast(0.5 as float));
SELECT * from tcast;
-- check shouldn't fail
EXPLAIN INSERT INTO tcast(url, price) values('www.yahoo.com', 0.5);
INSERT INTO tcast(url, price) values('www.yahoo.com', 0.5);
SELECT * FROM tcast;
DROP TABLE tcast;

-- complex expression
create table texpr(i int DEFAULT 89, f float NOT NULL ENABLE, d decimal(4,1),
    b boolean CHECK ((cast(d as float) + f) < cast(i as float) + (i*i)));
DESC FORMATTED texpr;
explain insert into texpr values(3,3.4,5.6,true);
insert into texpr values(3,3.4,5.6,true);
SELECT * from texpr;
DROP TABLE texpr;

-- UPDATE
create table acid_uami(i int,
                 de decimal(5,2) constraint nn1 not null enforced,
                 vc varchar(128) constraint ch2 CHECK de >= cast(i as decimal(5,2)) enforced)
                 clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED acid_uami;

-- insert as select
explain insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src;
insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src;

-- update
select * from acid_uami where de = 103.00 or de = 119.00;
explain update acid_uami set de = 893.14 where de = 103.00 or de = 119.00;
update acid_uami set de = 893.14 where de = 109.23 or de = 119.23;
select * from acid_uami where de = 103.00 or de = 119.00;

--ALTER table acid_uami drop constraint ch2;
--explain update acid_uami set vc = 'apache_hive' where de = 3.14 ;
--update acid_uami set de = 3.14159 where de = 3.14 ;
DROP TABLE acid_uami;



-- MERGE
-- multi insert
-- INSERT as SELECT (complicated queries)

-- drop constraint
CREATE TABLE numericDataType(a TINYINT CONSTRAINT tinyint_constraint DEFAULT 127Y ENABLE,
    b bigint CONSTRAINT check1 CHECK b in(4,5) ENABLE)
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED numericDataType;
ALTER TABLE numericDataType DROP CONSTRAINT check1;
DESC FORMATTED numericDataType;

EXPLAIN INSERT INTO numericDataType(b) values(456);
INSERT INTO numericDataType(b) values(456);
SELECT * from numericDataType;
DROP TABLE numericDataType;

-- column reference missing for column having check constraint
-- NULL for column with check shouldn't be possible
CREATE TABLE tcheck(a TINYINT, b bigint CONSTRAINT check1 CHECK b in(4,5) ENABLE)
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED tcheck;
EXPLAIN INSERT INTO tcheck(a) values(1);
EXPLAIN INSERT INTO tcheck(b) values(4);
INSERT INTO tcheck(b) values(4);
SELECT * FROM tcheck;
DROP TABLE tcheck;


