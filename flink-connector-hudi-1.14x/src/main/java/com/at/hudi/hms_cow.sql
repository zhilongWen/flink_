
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
    0: jdbc:hive2://hadoop102:10000> desc formatted user_behavior_hms_cow_tbl;
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
|           col_name            |                     data_type                      |                      comment                       |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
| # col_name                    | data_type                                          | comment                                            |
| _hoodie_commit_time           | string                                             |                                                    |
| _hoodie_commit_seqno          | string                                             |                                                    |
| _hoodie_record_key            | string                                             |                                                    |
| _hoodie_partition_path        | string                                             |                                                    |
| _hoodie_file_name             | string                                             |                                                    |
| user_id                       | int                                                |                                                    |
| item_id                       | bigint                                             |                                                    |
| category_id                   | int                                                |                                                    |
| behavior                      | string                                             |                                                    |
| ts                            | bigint                                             |                                                    |
|                               | NULL                                               | NULL                                               |
| # Partition Information       | NULL                                               | NULL                                               |
| # col_name                    | data_type                                          | comment                                            |
| dt                            | string                                             |                                                    |
| hh                            | string                                             |                                                    |
| mm                            | string                                             |                                                    |
|                               | NULL                                               | NULL                                               |
| # Detailed Table Information  | NULL                                               | NULL                                               |
| Database:                     | default                                            | NULL                                               |
| OwnerType:                    | USER                                               | NULL                                               |
| Owner:                        | zero                                               | NULL                                               |
| CreateTime:                   | Thu Jun 16 19:18:53 CST 2022                       | NULL                                               |
| LastAccessTime:               | UNKNOWN                                            | NULL                                               |
| Retention:                    | 0                                                  | NULL                                               |
| Location:                     | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl | NULL                                               |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                                               |
| Table Parameters:             | NULL                                               | NULL                                               |
|                               | EXTERNAL                                           | TRUE                                               |
|                               | last_commit_time_sync                              | 20220616191915729                                  |
|                               | numFiles                                           | 181                                                |
|                               | numPartitions                                      | 20                                                 |
|                               | numRows                                            | 0                                                  |
|                               | rawDataSize                                        | 0                                                  |
|                               | spark.sql.sources.provider                         | hudi                                               |
|                               | spark.sql.sources.schema.numPartCols               | 3                                                  |
|                               | spark.sql.sources.schema.numParts                  | 1                                                  |
|                               | spark.sql.sources.schema.part.0                    | {\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"item_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"category_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"behavior\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hh\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"mm\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]} |
|                               | spark.sql.sources.schema.partCol.0                 | dt                                                 |
|                               | spark.sql.sources.schema.partCol.1                 | hh                                                 |
|                               | spark.sql.sources.schema.partCol.2                 | mm                                                 |
|                               | totalSize                                          | 78392506                                           |
|                               | transient_lastDdlTime                              | 1655378333                                         |
|                               | NULL                                               | NULL                                               |
| # Storage Information         | NULL                                               | NULL                                               |
| SerDe Library:                | org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe | NULL                                               |
| InputFormat:                  | org.apache.hudi.hadoop.HoodieParquetInputFormat    | NULL                                               |
| OutputFormat:                 | org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat | NULL                                               |
| Compressed:                   | No                                                 | NULL                                               |
| Num Buckets:                  | 0                                                  | NULL                                               |
| Bucket Columns:               | []                                                 | NULL                                               |
| Sort Columns:                 | []                                                 | NULL                                               |
| Storage Desc Params:          | NULL                                               | NULL                                               |
|                               | hoodie.query.as.ro.table                           | false                                              |
|                               | path                                               | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl |
|                               | serialization.format                               | 1                                                  |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+



0: jdbc:hive2://hadoop102:10000> show create table user_behavior_hms_cow_tbl;
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `user_behavior_hms_cow_tbl`( |
|   `_hoodie_commit_time` string COMMENT '',         |
|   `_hoodie_commit_seqno` string COMMENT '',        |
|   `_hoodie_record_key` string COMMENT '',          |
|   `_hoodie_partition_path` string COMMENT '',      |
|   `_hoodie_file_name` string COMMENT '',           |
|   `user_id` int COMMENT '',                        |
|   `item_id` bigint COMMENT '',                     |
|   `category_id` int COMMENT '',                    |
|   `behavior` string COMMENT '',                    |
|   `ts` bigint COMMENT '')                          |
| PARTITIONED BY (                                   |
|   `dt` string COMMENT '',                          |
|   `hh` string COMMENT '',                          |
|   `mm` string COMMENT '')                          |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
| WITH SERDEPROPERTIES (                             |
|   'hoodie.query.as.ro.table'='false',              |
|   'path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl')  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hudi.hadoop.HoodieParquetInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl' |
| TBLPROPERTIES (                                    |
|   'last_commit_time_sync'='20220616191915729',     |
|   'spark.sql.sources.provider'='hudi',             |
|   'spark.sql.sources.schema.numPartCols'='3',      |
|   'spark.sql.sources.schema.numParts'='1',         |
|   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"user_id","type":"integer","nullable":true,"metadata":{}},{"name":"item_id","type":"long","nullable":true,"metadata":{}},{"name":"category_id","type":"integer","nullable":true,"metadata":{}},{"name":"behavior","type":"string","nullable":true,"metadata":{}},{"name":"ts","type":"long","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"hh","type":"string","nullable":true,"metadata":{}},{"name":"mm","type":"string","nullable":true,"metadata":{}}]}',  |
|   'spark.sql.sources.schema.partCol.0'='dt',       |
|   'spark.sql.sources.schema.partCol.1'='hh',       |
|   'spark.sql.sources.schema.partCol.2'='mm',       |
|   'transient_lastDdlTime'='1655378333')            |
+----------------------------------------------------+





alter table user_behavior_hms_cow_tbl add if not exists partition(`dt`='20220615',`hh`='20',`mm`='17') location 'hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl/20220615/20/17';

-- 修复表数据
msck repair table user_behavior_hms_cow_tbl;










