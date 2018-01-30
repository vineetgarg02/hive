--  SIMPLE TABLE
-- create table with first and last column with not null
CREATE TABLE table1 (a STRING NOT NULL ENFORCED, b STRING, c STRING NOT NULL ENFORCED);

-- insert value tuples
explain INSERT INTO table1 values('not', 'null', 'constraint');
INSERT INTO table1 values('not', 'null', 'constraint');
SELECT * FROM table1;

-- insert from select
explain INSERT INTO table1 select key, src.value, value from src;
INSERT INTO table1 select key, src.value, value from src;
SELECT * FROM table1;

-- insert overwrite
explain INSERT OVERWRITE TABLE table1 select src.*, value from src;
INSERT OVERWRITE TABLE table1 select src.*, value from src;
SELECT * FROM table1;

-- insert overwrite with if not exists
explain INSERT OVERWRITE TABLE table1 if not exists select src.key, src.key, src.value from src;
INSERT OVERWRITE TABLE table1 if not exists select src.key, src.key, src.value from src;
SELECT * FROM table1;

DROP TABLE table1;

-- multi insert
create table src_multi1 (a STRING NOT NULL ENFORCED, b STRING);
create table src_multi2 (i STRING, j STRING NOT NULL ENABLE);

explain
from src
insert overwrite table src_multi1 select * where key < 10
insert overwrite table src_multi2 select * where key > 10 and key < 20;


from src
insert overwrite table src_multi1 select * where key < 10
insert overwrite table src_multi2 select * where key > 10 and key < 20;

explain
from src
insert into table src_multi1 select * where src.key < 10
insert into table src_multi2 select src.* where key > 10 and key < 20;

from src
insert into table src_multi1 select * where src.key < 10
insert into table src_multi2 select src.* where key > 10 and key < 20;

--  ACID TABLE
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- SORT_QUERY_RESULTS
create table acid_uami(i int,
                 de decimal(5,2) constraint nn1 not null enforced,
                 vc varchar(128) constraint nn2 not null enforced) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert into values
explain insert into table acid_uami values
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');
insert into table acid_uami values
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');
select * from acid_uami;

 --insert into select
explain insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src;
insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src;

-- select with limit
explain insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src limit 2;

-- select with order by
explain insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src
        order by key limit 2;

-- select with group by
explain insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src
        group by key, value order by key limit 2;

 --overwrite
explain insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src;
insert into table acid_uami select cast(key as int), cast (key as decimal(5,2)), value from src;

-- update
explain update acid_uami set de = 3.14 where de = 109.23 or de = 119.23;
update acid_uami set de = 3.14 where de = 109.23 or de = 119.23;

ALTER table acid_uami drop constraint nn1;
ALTER table acid_uami CHANGE i i int constraint nn0 not null enforced;

explain update acid_uami set de = 3.14159 where de = 3.14 ;
update acid_uami set de = 3.14159 where de = 3.14 ;

-- multi insert
explain
from src
insert overwrite table acid_uami select cast(key as int), cast(key as decimal(5,2)), value where key < 10
insert overwrite table src_multi2 select * where key > 10 and key < 20;

set hive.exec.dynamic.partition.mode=nonstrict;
-- Table with partition
CREATE TABLE tablePartitioned (a STRING NOT NULL ENFORCED, b STRING, c STRING NOT NULL ENFORCED)
    PARTITIONED BY (p1 STRING, p2 INT NOT NULL ENABLE);

-- Insert into
explain INSERT INTO tablePartitioned partition(p1='today', p2=10) values('not', 'null', 'constraint');
INSERT INTO tablePartitioned partition(p1='today', p2=10) values('not', 'null', 'constraint');

-- Insert as select
explain INSERT INTO tablePartitioned partition(p1, p2) select key, value, value, key as p1, 3 as p2 from src limit 10;
INSERT INTO tablePartitioned partition(p1, p2) select key, value, value, key as p1, 3 as p2 from src limit 10;

-- partition is not based on alias but on position so constraints should be on appropriate columns
explain INSERT INTO tablePartitioned partition(p1, p2) select 1 as p1, 2 as p2, value, key, 3 from default.src limit 2;
INSERT INTO tablePartitioned partition(p1, p2) select 1 as p1, 2 as p2, value, key, 3 from default.src limit 2;

select * from tablePartitioned;

-- multi insert
explain
from src
INSERT INTO tablePartitioned partition(p1, p2) select 1 as p1, 2 as p2, value, key, 3 from default.src;
insert overwrite table src_multi2 select * where key > 10 and key < 20;

DROP TABLE src_multi1;
DROP TABLE src_multi2;
DROP TABLE acid_uami;

-- MERGE statements
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid (key int, a1 string, value string) stored as orc;

create table masking_test (key int NOT NULL enable, a1 string, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");

-- with cardinality check off
set hive.merge.cardinality.check=false;
explain MERGE INTO masking_test as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

-- with cardinality check on
set hive.merge.cardinality.check=true;
explain MERGE INTO masking_test as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

explain MERGE INTO masking_test as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

explain MERGE INTO masking_test as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

-- shouldn't have constraint enforcement
explain MERGE INTO masking_test as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE;

DROP TABLE masking_test;
DROP TABLE nonacid;
-- Test drop constraint
create table table2(i int constraint nn5 not null enforced, j);
explain insert into table2 values(2, 3);
alter table table2 drop constraint nn5;
explain insert into table2 values(2, 3);
DROP TABLE table2;

    -- Updates
-- TODO: CTAS

