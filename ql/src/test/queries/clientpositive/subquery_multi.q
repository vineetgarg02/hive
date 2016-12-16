create table tnull(i int, c char(2));
insert into tnull values(NULL, NULL), (NULL, NULL);

create table tempty(c char(2));
 
CREATE TABLE part_null(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
;

LOAD DATA LOCAL INPATH 'data/files/part_tiny_nulls.txt' overwrite into table part_null;

insert into part_null values(78487,NULL,'Manufacturer#6','Brand#52','LARGE BRUSHED BRASS', 23, 'MED BAG',1464.48,'hely blith');

-- Both IN are always true so should return all rows
explain select * from part_null where p_size IN (select p_size from part_null) AND p_brand IN (select p_brand from part_null);
select * from part_null where p_size IN (select p_size from part_null) AND p_brand IN (select p_brand from part_null);

-- NOT IN has null value so should return 0 rows
explain select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_name from part_null);
select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_name from part_null);

-- NOT IN is always true and IN is false for where p_name is NULL, hence should return all but one row
select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_type from part_null);

-- NOT IN has one NULL value so this whole query should not return any row
select * from part_null where p_brand IN (select c from tnull) AND p_brand NOT IN (select p_name from part_null);

-- NOT IN is always true irrespective of p_name being null/non-null since inner query is empty
-- second query is always true so this should return all rows
select * from part_null where p_name NOT IN (select c from tempty) AND p_brand IN (select p_brand from part_null);

select * from part_null where p_size IN (select p_size from part_null) AND NOT EXISTS (select i from tnull);

select * from part_null where p_name NOT IN (select p_name from part_null) AND EXISTS (select i from tnull);

drop table tnull;
drop table tempty;:
drop table part_null;

--same corr var in more than 3 queries (all reffering to same outer var)
explain select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type) AND p_brand IN (select p_brand from part where part.p_type = part_null.p_type);

-- (subq1) AND (subq2 OR pred)

--diff corr var in more than 3 queries (all reffering to diff outer var)
select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type) AND p_brand NOT IN (select p_type from part where part.p_size = part_null.p_size); 

--mix corr var in more than 3 queries (two reffering to same outer var)


