--! qt:dataset:part

--empty table
create table tempty(i int, j int);

CREATE TABLE part_null_n0 as select * from part;
insert into part_null_n0 values(NULL,NULL,NULL,NULL,NULL, NULL, NULL,NULL,NULL);

-- test all six comparison operators
--explain cbo select count(*) from part where p_partkey = ALL (select p_partkey from part);
--select count(*) from part where p_partkey = ALL (select p_partkey from part);

explain cbo select count(*) from part where p_partkey <> ALL (select p_partkey from part);
select count(*) from part where p_partkey <> ALL (select p_partkey from part);

explain cbo select count(*) from part where p_partkey > ALL (select p_partkey from part);
select count(*) from part where p_partkey > ALL (select p_partkey from part);

explain cbo select count(*) from part where p_partkey < ALL (select p_partkey from part);
select count(*) from part where p_partkey < ALL (select p_partkey from part);

explain cbo select count(*) from part where p_partkey >= ALL (select p_partkey from part);
select count(*) from part where p_partkey >= ALL (select p_partkey from part);

explain cbo select count(*) from part where p_partkey <= ALL (select p_partkey from part);
select count(*) from part where p_partkey <= ALL (select p_partkey from part);

-- ALL with aggregate in subquery
explain cbo select count(*) from part where p_size < ALL (select max(p_size) from part group by p_partkey);
select count(*) from part where p_size < ALL (select max(p_size) from part group by p_partkey);

select count(*) from part where p_size < ALL (select max(null) from part group by p_partkey);

--empty row produces true with ALL
select count(*) from part where p_partkey <> ALL(select i from tempty);

-- true + null, should produce zero results 
select count(*) from part where p_partkey > ALL (select max(p_partkey) from part_null_n0 UNION select null from part group by true);

-- false + null -> false, therefore should produce results
select count(*) from part where ((p_partkey <> ALL (select p_partkey from part_null_n0)) == false);

-- all null -> null
select count(*) from part where (p_partkey <> ALL (select p_partkey from part_null_n0 where p_partkey is null)) is null;

-- false, should produce zero result
select count(*) from part where p_partkey > ALL (select max(p_partkey) from part_null_n0);

-- ALL in having
explain cbo select count(*) from part having count(*) > ALL (select count(*) from part group by p_partkey);
select count(*) from part having count(*) > ALL (select count(*) from part group by p_partkey);

-- multiple
explain cbo select count(*) from part where p_partkey >= ALL (select p_partkey from part) 
	AND p_size <> ALL (select p_size from part group by p_size);
select count(*) from part where p_partkey >= ALL (select p_partkey from part) 
	AND p_partkey <> ALL (select p_size from part group by p_size);

--nested
explain cbo select count(*) from part where p_partkey 
	>= ALL (select p_partkey from part where p_size >= ALL(select p_size from part_null_n0 group by p_size)) ;
select count(*) from part where p_partkey 
	>= ALL (select p_partkey from part where p_size >= ALL(select p_size from part_null_n0 group by p_size)) ;
