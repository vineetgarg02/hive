set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table tx1 (a integer,b integer);
insert into tx1	values  (1, 105),
                        (2, 203),
                        (1, 300);

create table tx2 (a int, b int);
insert into tx2 values (1,1);


select * from tx1 u left semi join tx1 v on u.a=v.a ;
select * from tx1 u join tx2 v on u.a=v.a and u.b <> v.b;

create table t1 as select cast(key as int) key, value from src where key <= 10;

select * from t1 sort by key;

create table t2 as select cast(2*key as int) key, value from t1;

select * from t2 sort by key;

create table t3 as select * from (select * from t1 union all select * from t2) b;
select * from t3 sort by key, value;

create table t4 (key int, value string);
select * from t4;

select * from t1 a join t2 b on a.key=b.key ;
select * from t1 a left join t2 b on a.key=b.key ;

select * from t1 a join t2 b on a.key=b.key and a.value > b.value;
select * from t1 a left outer join t2 b on a.key=b.key  and a.value > b.value;

select * from t1 a left semi join t2 b on a.key=b.key ;


drop table t1;
drop table t2;
create table t1(i int, j int);
insert into t1 values(4,1);

create table t2(i int, j int);
insert into t2 values(4,2),(4,3),(4,5);

explain select * from t1 left semi join t2 on t1.i = t2.i and t2.j <> t1.j;
select * from t1 left semi join t2 on t1.i = t2.i and t2.j <> t1.j;

drop table t1;
drop table t2;



