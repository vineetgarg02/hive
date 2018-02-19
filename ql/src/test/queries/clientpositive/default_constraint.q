-- create table
 -- numeric type
 set hive.stats.autogather=false;
CREATE TABLE numericDataType(a TINYINT CONSTRAINT tinyint_constraint DEFAULT 127Y ENABLE, b SMALLINT DEFAULT 32767S, c INT DEFAULT 2147483647,
    d BIGINT DEFAULT  9223372036854775807L, e DOUBLE DEFAULT 3.4E38, f DECIMAL(9,2) DEFAULT 1234567.89);
DESC FORMATTED numericDataType;

EXPLAIN INSERT INTO numericDataType(a) values(3Y);
INSERT INTO numericDataType(a) values(3Y);
SELECT * FROM numericDataType;

EXPLAIN INSERT INTO numericDataType(e,f) values(4.5, 678.4);
INSERT INTO numericDataType(e,f) values(4.5, 678.4);
SELECT * FROM numericDataType;

  -- Date/time
CREATE TABLE table1(d DATE DEFAULT DATE'2018-02-14', t TIMESTAMP DEFAULT "2016-02-22 12:45:07.000000000",
    d1 DATE DEFAULT current_date() ENABLE, t1 TIMESTAMP DEFAULT current_timestamp() DISABLE);
DESC FORMATTED table1;

EXPLAIN INSERT INTO table1(t) values ("1985-12-31 12:45:07");
INSERT INTO table1(t) values ("1985-12-31 12:45:07");
SELECT d, t, d1=current_date(), t1 from table1;

EXPLAIN INSERT INTO table1(d, t1) values ("1985-12-31", current_timestamp());
INSERT INTO table1(d, t1) values ("1985-12-31", current_timestamp());
SELECT d, t, d1=current_date(), t1=current_timestamp() from table1;

--TODO: alter table table1 to enable constraint
-- reinsert data

-- string type
CREATE TABLE table2(i STRING DEFAULT current_database(), j STRING DEFAULT current_user(),
    k STRING DEFAULT 'current_user()', v varchar(350) DEFAULT 'varchar_default_value', c char(20) DEFAULT 'char_value');
DESC FORMATTED table2;

-- misc type
CREATE TABLE misc(b BOOLEAN DEFAULT true, b1 BINARY DEFAULT 'bin');
DESC FORMATTED misc;

-- alter table drop and add constraint
ALTER TABLE numericDataType DROP CONSTRAINT tinyint_constraint;
DESC FORMATTED numericDataType;
ALTER TABLE numericDataType ADD CONSTRAINT uk1 UNIQUE(a,b) DISABLE NOVALIDATE;
DESC FORMATTED numericDataType;

-- alter table change column with constraint
ALTER TABLE numericDataType CHANGE a a TINYINT CONSTRAINT second_constraint DEFAULT 127Y ENABLE;
DESC FORMATTED numericDataType;

-- alter table add column with constraint
--XXX: Existing BUG ADD column not working
--ALTER TABLE misc ADD COLUMNS (a STRING);
--DESC FORMATTED misc;
ALTER TABLE misc CHANGE b1 b1 BINARY DEFAULT 'newValue';
DESC FORMATTED misc;

--DROP COLUMN with constraint XXX BUG
-- ALTER TABLE misc REPLACE COLUMNS(b BOOLEAN);
-- DESC FORMATTED misc;

DROP TABLE numericDataType;
DROP TABLE table1;
DROP TABLE table2;
DROP TABLE misc;


