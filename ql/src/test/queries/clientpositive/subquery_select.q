-- following tests test queries in SELECT
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

-- IN, non corr
explain SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part)
FROM part;

SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part)
FROM part ;

-- IN, corr
EXPLAIN SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

-- NOT IN, non corr
explain SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part)
FROM part;

SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part)
FROM part ;

-- NOT IN, corr
EXPLAIN SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

-- EXISTS, non corr
explain SELECT p_size, EXISTS(SELECT p_size FROM part)
FROM part;

SELECT p_size, EXISTS(SELECT p_size FROM part)
FROM part;

-- EXISTS, corr
explain SELECT p_size, EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

SELECT p_size, EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

-- NOT EXISTS, non corr
explain SELECT p_size, NOT EXISTS(SELECT p_size FROM part)
FROM part;

SELECT p_size, NOT EXISTS(SELECT p_size FROM part)
FROM part;

-- NOT EXISTS, corr
explain SELECT p_size, NOT EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

SELECT p_size, NOT EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

-- scalar with COUNT, since where is always false count should return 0
explain select p_size, (select count(p_name) from part p where p.p_type = part.p_name) from part;
select p_size, (select count(p_name) from part p where p.p_type = part.p_name) from part;

-- scalar with MAX, since where is always false max should return NULL
explain select p_size, (select max(p_name) from part p where p.p_type = part.p_name) from part;
select p_size, (select max(p_name) from part p where p.p_type = part.p_name) from part;

-- SCALAR, non corr
explain SELECT p_size, (SELECT max(p_size) FROM part)
    FROM part;

SELECT p_size, (SELECT max(p_size) FROM part)
    FROM part;

-- IN, corr with scalar
explain
select *
from src b
where b.key in
        (select (select max(key) from src)
         from src a
         where b.value = a.value and a.key > '9'
        );
select *
from src b
where b.key in
        (select (select max(key) from src)
         from src a
         where b.value = a.value and a.key > '9'
        );

-- corr within corr..correcionnn..
explain
select *
from src b
where b.key in
        (select (select max(key) from src sc where sc.value = a.value)
         from src a
         where b.value = a.value and a.key > '9'
        );

select *
from src b
where b.key in
        (select (select max(key) from src sc where sc.value = a.value)
         from src a
         where b.value = a.value and a.key > '9' );

CREATE table tnull(i int);
insert into tnull values(null);

-- IN query returns unknown/NULL instead of true/false
explain select p_size, p_size IN (select i from tnull) from part;
select p_size, p_size IN (select i from tnull) from part;

CREATE TABLE tempty(i int);

explain select p_size, (select count(*) from tempty) from part;
select p_size, (select count(*) from tempty) from part;

explain select p_size, (select max(i) from tempty) from part;
select p_size, (select max(i) from tempty) from part;

DROP table tempty;
DROP table tnull;

-- following tests test subquery in all kind of expressions (except UDAF, UDA and UDTF)

-- explain select (select max(p_size) from part);
-- select (select max(p_size) from part);

-- different data types
-- string with string
-- null with int
-- boolean (IN, EXISTS) with AND, OR

-- scalar, corr
explain SELECT p_size, 1+(SELECT max(p_size) FROM part p
    WHERE p.p_type = part.p_type) from part;
SELECT p_size, 1+(SELECT max(p_size) FROM part p
    WHERE p.p_type = part.p_type) from part;

-- IS NULL
explain SELECT p_size, (SELECT count(p_size) FROM part p
    WHERE p.p_type = part.p_type) IS NULL from part;
SELECT p_size, (SELECT count(p_size) FROM part p
    WHERE p.p_type = part.p_type) IS NULL from part;

-- scalar, non-corr, non agg
explain select p_type, (select p_size from part order by p_size limit 1) = 1 from part;
select p_type, (select p_size from part order by p_size limit 1) = 1 from part;



