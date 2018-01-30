set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid (key int, value string) stored as orc;

create table test1(key int, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");

MERGE INTO test1 as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set key = 1
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.value);
