set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table tx1 (a integer,b integer);
insert into tx1	values  (1, 105),
                        (2, 203),
                        (3, 300);

create table tx2 (a int, b int);
insert into tx2 values (1,105);
insert into tx2 values (1,1995);


explain select * from tx1 u left semi join tx2 v on u.a=v.a and u.b <> v.b ;
select * from tx1 u left semi join tx2 v on u.a=v.a and u.b <> v.b ;
select * from tx1 u join tx2 v on u.a=v.a and u.b <> v.b;
select * from tx1 u left semi join tx1 v on u.a=v.a ;

