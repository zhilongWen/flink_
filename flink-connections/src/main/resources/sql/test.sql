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


CREATE TABLE IF NOT EXISTS user_behavior_tbl
(
    user_id     bigint,
    item_id     int,
    category_id bigint,
    behavior string,
    ts          bigint
) comment 'UserBehavior.csv' partitioned by (`dt` STRING,`hm` STRING)
    stored as orc
    location '/user/hive/warehouse/user_behavior_tbl'
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


-- file connector read UserBehavior.csv
create table if not exists file_soure_tbl
(
    user_id     bigint,
    item_id     int,
    category_id bigint,
    behavior string,
    ts          bigint
        row_time as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')),
    watermark for row_time as row_time - interval '1' second
)
with (
    'connector' = 'filesystem',
    'path' = 'file:///D:/workspace/flink_/files/UserBehavior.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
)

-- create hive table
create table if not exists user_behavior_tbl
(
    user_id     bigint,
    item_id     int,
    category_id bigint,
    behavior string,
    ts          bigint
) comment 'UserBehavior.csv' partitioned by (`dt` string,`hm` string)
    stored as orc
    location '/user/hive/warehouse/user_behavior_tbl'
    tblproperties(
        'orc.compress' = 'snappy',
        'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
        'sink.partition-commit.trigger'='partition-time',
        'sink.partition-commit.delay'='5 min',
        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- Assume user configured time zone is 'Asia/Shanghai'
        'sink.partition-commit.policy.kind'='metastore,success-file'
    )

-- insert
insert into user_behavior_tbl
select user_id,
       item_id,
       category_id,
       behavior,
       ts,
       DATE_FORMAT(row_time, 'yyyyMMdd'),
       DATE_FORMAT(row_time, 'HH')
from file_soure_tbl



drop table if exists file_soure_tbl



CREATE TABLE kafka_source_tbl
(
    `userId`     INT,
    `itemId`     BIGINT,
    `categoryId` INT,
    `behavior` STRING,
    `ts`         BIGINT,
    row_time     AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),
    WATERMARK FOR row_time AS row_time - INTERVAL '1' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'user_behaviors',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'properties.group.id' = 'test-group-id',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);


create
catalog myhive with('type'='hive','hive-conf-dir'='/opt/module/hive-3.1.2/conf','default-database'='default','hive-version'='3.1.2','hadoop-conf-dir'='/opt/module/hadoop-3.1.3/etc/hadoop');
use catalog myhive;
load
module hive;
use modules hive,core;

set table.sql -dialect = hive;


CREATE TABLE if not exists kafka_source_tbl
(
    `userId`     INT,
    `itemId`     BIGINT,
    `categoryId` INT,
    `behavior` STRING,
    `ts`         BIGINT,
    row_time     AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),
    WATERMARK FOR row_time AS row_time - INTERVAL '1' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'user_behaviors',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'properties.group.id' = 'test-group-id',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);


insert into user_behavior_tbl
select CAST(userId AS BIGINT)     as user_id,
       CAST(itemId AS INT)        as item_id,
       CAST(categoryId AS BIGINT) as category_id,
       behavior,
       ts,
       DATE_FORMAT(row_time, 'yyyyMMdd'),
       DATE_FORMAT(row_time, 'HH')
from kafka_source_tbl



create table if not exists user_behavior_tbl
(
    user_id     bigint,
    item_id     int,
    category_id bigint,
    behavior string,
    ts          bigint,
    `dt` string,
    `hm` string
) partitioned by (`dt`,`hm`)
with(
    'connector'='filesystem',
    'path'='hdfs://hadoop102:8020/user/hive/warehouse/user_behavior_tbl/',
    'format'='orc',
    'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',
    'sink.partition-commit.delay'='1 min',
    'sink.partition-commit.trigger'='partition-time',
    'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- Assume user configured time zone is 'Asia/Shanghai'
    'sink.partition-commit.policy.kind'='success-file'
)



load data inpath '/user/hive/warehouse/user_behavior_tbl/' into table default.student







