-- create table with first and last column with not null
CREATE TABLE table1 (a STRING NOT NULL ENFORCED, b STRING, c STRING NOT NULL ENFORCED);

-- insert value tuples
explain INSERT INTO table1 values('not', 'null', 'constraint');

-- insert from select
explain INSERT INTO table1 select key, src.value, value from src;

-- insert overwrite
explain INSERT OVERWRITE TABLE table1 select src.*, value from src;

-- insert overwrite with if not exists
explain INSERT OVERWRITE TABLE table1 if not exists select src.key, src.key, src.value from src;

-- multi insert
create table src_multi1 (a STRING NOT NULL ENFORCED, b STRING);
create table src_multi2 (i STRING, j STRING NOT NULL DISABLE);

explain
from src
insert overwrite table src_multi1 select * where key < 10
insert overwrite table src_multi2 select * where key > 10 and key < 20;

explain
from src
insert into table src_multi1 select * where src.key < 10
insert into table src_multi2 select src.* where key > 10 and key < 20;
