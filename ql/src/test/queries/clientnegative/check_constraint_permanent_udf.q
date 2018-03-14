-- user defined permament UDFs are not allowed
CREATE FUNCTION test_udf2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';
CREATE TABLE tudf(v string CHECK test_udf2(v) <> 'vin');