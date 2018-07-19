--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.tez.cartesian-product.enabled=true;
set hive.auto.convert.join=true;
set hive.convert.join.bucket.mapjoin.tez=true;

create table X_n2 (key string, value string) clustered by (key) into 2 buckets;
insert overwrite table X_n2 select distinct * from src order by key limit 10;

create table X_n3 (key1 string, value1 string) clustered by (key1) into 2 buckets;
insert overwrite table X_n2 select distinct * from src order by key limit 10;

create table Y_n0 as
select * from src order by key limit 1;

explain select * from Y_n0, (select * from X_n2 as A join X_n3 as B on A.key=B.key1) as C where Y_n0.key=C.key;
