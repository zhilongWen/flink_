
/*
 	//https://blog.csdn.net/dudu146863/article/details/120151688
	//https://blog.csdn.net/weixin_44131414/article/details/122983339
	https://www.jianshu.com/p/0cf7cd00eb00

drop table hudi_user_behavior_tbl;
drop table user_behavior_hms_mor;
drop table user_behavior_hms_mor_tbl_ro;
drop table user_behavior_hms_mor_tbl_rt;



hadoop fs -rm -r /user/warehouse/user_behavior_hms_mor_tbl

 */


create table if not exists hudi_user_behavior_tbl
(
    userId     int,
    itemId     bigint,
    categoryId int,
    behavior string,
    ts         bigint,
    row_time   as to_timestamp(from_unixtime(ts / 1000,'yyyy-MM-dd HH:mm:ss')),
    watermark for row_time as row_time - interval '1' second
)
with (
    'connector' = 'kafka',
    'topic' = 'user_behaviors',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'properties.group.id' = 'test-group-id',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)



create table if not exists user_behavior_hms_cow
(
    user_id     int,
    item_id     bigint,
    category_id int,
    behavior string,
    ts          bigint,
    `dt` string,
    `hh` string,
    `mm` string
)partitioned by (`dt`,`hh`,`mm`)
with(
    'connector'='hudi',
    'path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl',
    'table.type'='COPY_ON_WRITE',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出
    'hoodie.datasource.write.recordkey.field' = 'user_id',
    'write.precombine.field'= 'ts',
    'write.tasks'='1',
    'write.rate.limit'= '100',
    'compaction.tasks'='1',
    'compaction.async.enabled'= 'true',
    'compaction.trigger.strategy'= 'num_and_time',
    'compaction.delta_commits'= '1',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc
    'hive_sync.enable'='true',           -- required，开启hive同步功能
    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,
    'hive_sync.table'='user_behavior_hms_cow_tbl',                          -- required, hive 新建的表名
    'hive_sync.db'='default',                       -- required, hive 新建的数据库名
    'hive_sync.support_timestamp'= 'true'
)


insert into user_behavior_hms_cow
select
    userId as user_id,
    itemId as item_id,
    categoryId as category_id,
    behavior,
    ts,
    date_format(cast(row_time as string),'yyyyMMdd') dt,
    date_format(cast(row_time as string),'HH') hh,
    date_format(cast(row_time as string),'mm') mm
from hudi_user_behavior_tbl


-- ==============================================================================================
-- hive table
-- ==============================================================================================












