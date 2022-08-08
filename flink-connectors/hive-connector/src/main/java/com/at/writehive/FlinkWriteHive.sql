


-- flink kafka source table

SET table.sql-dialect=default;

create table if not exists kafka_source_tbl(
   id bigint,
   name string,
   address string,
   ts bigint,
   row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000),'yyyy-MM-dd HH:mm:ss'),
   watermark for row_time as row_time - interval '60' second
)with(
    'connector' = 'kafka',
    'topic' = 'hive-test-logs',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'properties.group.id' = 'hive-logs-group-id',
    -- 'scan.startup.mode' = 'earliest-offset',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)

-- hive sink table

SET table.sql-dialect=hive;

create table if not exists hive_stream_create_flink_tbl(
       id bigint,
       name string,
       address string,
       ts bigint
)COMMENT 'flink create table test'
    PARTITIONED BY (`dt` STRING,`hm` STRING,`mm` STRING)
    STORED AS ORC
    LOCATION '/warehouse/test/hive_stream_create_flink_tbl'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'partition.time-extractor.timestamp-pattern'='$dt $hr:$mm:00',
        'sink.partition-commit.trigger'='partition-time',
        'sink.partition-commit.delay'='1 min',
        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',
        'sink.partition-commit.policy.kind'='metastore,success-file'
    )


-- insert

SET table.sql-dialect=default;

insert into hive_stream_create_flink_tbl
select
    id,
    name,
    address,
    ts,
    date_format(row_time,'yyyy-MM-dd'),
    date_format(row_time,'HH'),
    date_format(row_time,'mm')
from kafka_source_tbl





