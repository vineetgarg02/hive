set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid (key int, a1 string, value string) stored as orc;
insert into nonacid values(1, null, 'value');

create table test1 (key int NOT NULL enable, a1 string, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");

insert into test1 values(2,  'a1', 'value'); 

MERGE INTO test1 as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);
