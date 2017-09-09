-- to test cardinality of filter in physical planning
set hive.stats.fetch.column.stats=true;
set hive.explain.user=false;

CREATE TABLE mytable
(
num1 INT,
num2 INT,
num3 INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

LOAD DATA LOCAL inpath "../../data/files/filterCard.txt" into table mytable;

ANALYZE table mytable compute statistics for columns;

explain select * from mytable where num1=4;
explain select * from mytable where num1=4 and num2=8;
explain select * from mytable where num1=4 and num2=8 and num3=7;

drop table mytable;
