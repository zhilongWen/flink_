/*

./bin/yarn-session.sh -nm flink-job -jm 1024m -tm 4096 -s 4 -Dyarn.provided.lib.dirs='hdfs://hadoop102:8020/flink-remote-dir/lib' -qu hive -d


-- init.sql
create catalog myhive with('type'='hive','hive-conf-dir'='/opt/module/hive-3.1.2/conf','default-database'='default','hive-version'='3.1.2','hadoop-conf-dir'='/opt/module/hadoop-3.1.3/etc/hadoop');
use catalog myhive;
load module hive;
use modules hive,core;
set table.sql-dialect=hive;


./bin/sql-client.sh -s yarn-session -i init.sql -j ./lib/*

*/

set table.sql-dialect=hive;

set table.sql-dialect=default;

SET 'execution.runtime-mode' = 'streaming';

SET 'parallelism.default'='4';

SET 'sql-client.execution.result-mode' = 'tableau';

SET 'state.backend'='filesystem';
SET 'state.checkpoints.dir'='hdfs://hadoop102:8020/tmp/flink-checkpoints';
SET 'execution.checkpointing.mode'='EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '1 min';
SET 'execution.checkpointing.timeout'='1 min';
SET 'execution.checkpointing.tolerable-failed-checkpoints'='10';
SET 'execution.checkpointing.externalized-checkpoint-retention'='RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.max-concurrent-checkpoints'='1';
SET 'execution.checkpointing.unaligned'='true';

SET 'rest.flamegraph.enabled'='true';



-- create kafka source table

set table.sql-dialect=default;


create table if not exists kafka_source_tbl(
   id bigint,
   name string,
   address string,
   ts bigint,
   row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000),'yyyy-MM-dd HH:mm:ss'),
   watermark for row_time as row_time - interval '60' second
)with(
    'connector' = 'kafka',
    'topic' = 'flink-write-hive-test-topic',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'properties.group.id' = 'flink-write-hive-test-topic-groupId',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);



-- create hive sink table

set table.sql-dialect=hive;

create table if not exists hive_stream_create_flink_timestamp_formatter_tbl(
       id bigint,
       name string,
       address string,
       ts bigint
)COMMENT 'flink create table test'
    PARTITIONED BY (`dt` STRING,`hm` STRING,`mm` STRING)
    STORED AS ORC
    LOCATION '/warehouse/test/hive_stream_create_flink_timestamp_formatter_tbl'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'partition.time-extractor.timestamp-pattern'='$dt $hm:$mm:00',
       ---- 'partition.time-extractor.timestamp-formatter'='yyyyMMdd HH:mm:ss',
        'sink.partition-commit.trigger'='partition-time',
        'sink.partition-commit.delay'='1 min',
        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',
        'sink.partition-commit.policy.kind'='metastore,success-file',
        'sink.rolling-policy.file-size'='1MB',
        'sink.rolling-policy.rollover-interval'='1 min',
        'compaction.file-size'='1MB',
        'sink.rolling-policy.rollover-interval'='1 min'
    );


-- insert

set table.sql-dialect=default;

insert into hive_stream_create_flink_timestamp_formatter_tbl
select
    id,
    name,
    address,
    ts,
    date_format(row_time,'yyyy-MM-dd'),
    date_format(row_time,'HH'),
    date_format(row_time,'mm')
from kafka_source_tbl





