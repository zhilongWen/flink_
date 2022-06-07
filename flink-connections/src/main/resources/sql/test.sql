CREATE TABLE IF NOT EXISTS rule_tbl
(
    user_id int,
    name string,
    age     int,
    sex     int,
    address string
) comment '测试flink读写hive数据' partitioned by (`dt` STRING,`hm` STRING)
    stored as orc
    location '/user/hive/warehouse/rule_tbl'
    tblproperties("orc.compress" = "snappy")

insert into rule_tbl partition (dt='20220607',hm='2000')
values (1016727, 'Mary', 10, 1, '中国北京市')


insert into rule_tbl
select user_id,
       name,
       age,
       sex,
       address,
       from_unixtime(unix_timestamp(), 'yyyyMMdd') as dt,
       '2100'                                      as hm
from source_tbl






