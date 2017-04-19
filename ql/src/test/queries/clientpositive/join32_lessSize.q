set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=6000;


EXPLAIN EXTENDED
SELECT x.key, z.value, y.value
FROM src w JOIN src1 x ON (x.value = w.value)
JOIN src y ON (x.key = y.key) ;

EXPLAIN EXTENDED
SELECT x.key, z.value, y.value
FROM src w JOIN src1 x ON (x.value = w.value) 
JOIN src y ON (x.key = y.key) 
JOIN src1 z ON (x.key = z.key);

SELECT x.key, z.value, y.value
FROM src w JOIN src1 x ON (x.value = w.value) 
JOIN src y ON (x.key = y.key) 
JOIN src1 z ON (x.key = z.key);
