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

create table if not exists file_soure_tbl
(
    user_id     bigint,
    item_id     bigint,
    category_id bigint,
    behavior string,
    ts          bigint,
    row_time    as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')),
    watermark for row_time as row_time - interval '1' second
)
with (
    'connector' = 'filesystem',
    'path' = 'file:///D:/workspace/flink_/files/UserBehavior.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
)







