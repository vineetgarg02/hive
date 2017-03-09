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

-- SCALAR, corr
explain SELECT p_size, (SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)
FROM part;

SELECT p_size, (SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)
FROM part;

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
