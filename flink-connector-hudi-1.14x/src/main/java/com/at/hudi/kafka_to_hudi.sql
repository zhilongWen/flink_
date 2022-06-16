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



create table if not exists user_behavior_hms_mor
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
    'path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl',
    'table.type'='MERGE_ON_READ',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出
    'hoodie.datasource.write.recordkey.field' = 'user_id',
    'write.precombine.field'= 'ts',
    'write.tasks'='1',
    'write.rate.limit'= '2000',
    'compaction.tasks'='1',
    'compaction.async.enabled'= 'true',
    'compaction.trigger.strategy'= 'num_and_time',
    'compaction.delta_commits'= '1',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc
    'hive_sync.enable'='true',           -- required，开启hive同步功能
    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,
    'hive_sync.table'='user_behavior_hms_mor_tbl',                          -- required, hive 新建的表名
    'hive_sync.db'='default',                       -- required, hive 新建的数据库名
    'hive_sync.support_timestamp'= 'true'
)


insert into user_behavior_hms_mor
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


-- ================================================ user_behavior_hms_mor ==============================================

0: jdbc:hive2://hadoop102:10000> desc formatted user_behavior_hms_mor;
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
|           col_name            |                     data_type                      |                      comment                       |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
| # col_name                    | data_type                                          | comment                                            |
|                               | NULL                                               | NULL                                               |
| # Detailed Table Information  | NULL                                               | NULL                                               |
| Database:                     | default                                            | NULL                                               |
| OwnerType:                    | USER                                               | NULL                                               |
| Owner:                        | null                                               | NULL                                               |
| CreateTime:                   | Thu Jun 16 16:32:56 CST 2022                       | NULL                                               |
| LastAccessTime:               | UNKNOWN                                            | NULL                                               |
| Retention:                    | 0                                                  | NULL                                               |
| Location:                     | hdfs://hadoop102:8020/user/hive/warehouse/user_behavior_hms_mor | NULL                                               |
| Table Type:                   | MANAGED_TABLE                                      | NULL                                               |
| Table Parameters:             | NULL                                               | NULL                                               |
|                               | flink.compaction.async.enabled                     | true                                               |
|                               | flink.compaction.delta_commits                     | 1                                                  |
|                               | flink.compaction.tasks                             | 1                                                  |
|                               | flink.compaction.trigger.strategy                  | num_and_time                                       |
|                               | flink.connector                                    | hudi                                               |
|                               | flink.hive_sync.db                                 | default                                            |
|                               | flink.hive_sync.enable                             | true                                               |
|                               | flink.hive_sync.metastore.uris                     | thrift://hadoop102:9083                            |
|                               | flink.hive_sync.mode                               | hms                                                |
|                               | flink.hive_sync.support_timestamp                  | true                                               |
|                               | flink.hive_sync.table                              | user_behavior_hms_mor_tbl                          |
|                               | flink.hoodie.datasource.write.recordkey.field      | user_id                                            |
|                               | flink.partition.keys.0.name                        | dt                                                 |
|                               | flink.partition.keys.1.name                        | hh                                                 |
|                               | flink.partition.keys.2.name                        | mm                                                 |
|                               | flink.path                                         | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl |
|                               | flink.read.streaming.check-interval                | 4                                                  |
|                               | flink.read.streaming.enabled                       | true                                               |
|                               | flink.schema.0.data-type                           | INT                                                |
|                               | flink.schema.0.name                                | user_id                                            |
|                               | flink.schema.1.data-type                           | BIGINT                                             |
|                               | flink.schema.1.name                                | item_id                                            |
|                               | flink.schema.2.data-type                           | INT                                                |
|                               | flink.schema.2.name                                | category_id                                        |
|                               | flink.schema.3.data-type                           | VARCHAR(2147483647)                                |
|                               | flink.schema.3.name                                | behavior                                           |
|                               | flink.schema.4.data-type                           | BIGINT                                             |
|                               | flink.schema.4.name                                | ts                                                 |
|                               | flink.schema.5.data-type                           | VARCHAR(2147483647)                                |
|                               | flink.schema.5.name                                | dt                                                 |
|                               | flink.schema.6.data-type                           | VARCHAR(2147483647)                                |
|                               | flink.schema.6.name                                | hh                                                 |
|                               | flink.schema.7.data-type                           | VARCHAR(2147483647)                                |
|                               | flink.schema.7.name                                | mm                                                 |
|                               | flink.table.type                                   | MERGE_ON_READ                                      |
|                               | flink.write.precombine.field                       | ts                                                 |
|                               | flink.write.rate.limit                             | 2000                                               |
|                               | flink.write.tasks                                  | 1                                                  |
|                               | transient_lastDdlTime                              | 1655368376                                         |
|                               | NULL                                               | NULL                                               |
| # Storage Information         | NULL                                               | NULL                                               |
| SerDe Library:                | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe | NULL                                               |
| InputFormat:                  | org.apache.hadoop.mapred.TextInputFormat           | NULL                                               |
| OutputFormat:                 | org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat | NULL                                               |
| Compressed:                   | No                                                 | NULL                                               |
| Num Buckets:                  | -1                                                 | NULL                                               |
| Bucket Columns:               | []                                                 | NULL                                               |
| Sort Columns:                 | []                                                 | NULL                                               |
| Storage Desc Params:          | NULL                                               | NULL                                               |
|                               | serialization.format                               | 1                                                  |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+

0: jdbc:hive2://hadoop102:10000> show create table user_behavior_hms_mor;
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE TABLE `user_behavior_hms_mor`(              |
    | )                                                  |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hadoop.mapred.TextInputFormat'       |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat' |
| LOCATION                                           |
|   'hdfs://hadoop102:8020/user/hive/warehouse/user_behavior_hms_mor' |
| TBLPROPERTIES (                                    |
|   'flink.compaction.async.enabled'='true',         |
|   'flink.compaction.delta_commits'='1',            |
|   'flink.compaction.tasks'='1',                    |
|   'flink.compaction.trigger.strategy'='num_and_time',  |
|   'flink.connector'='hudi',                        |
|   'flink.hive_sync.db'='default',                  |
|   'flink.hive_sync.enable'='true',                 |
|   'flink.hive_sync.metastore.uris'='thrift://hadoop102:9083',  |
|   'flink.hive_sync.mode'='hms',                    |
|   'flink.hive_sync.support_timestamp'='true',      |
|   'flink.hive_sync.table'='user_behavior_hms_mor_tbl',  |
|   'flink.hoodie.datasource.write.recordkey.field'='user_id',  |
|   'flink.partition.keys.0.name'='dt',              |
|   'flink.partition.keys.1.name'='hh',              |
|   'flink.partition.keys.2.name'='mm',              |
|   'flink.path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl',  |
|   'flink.read.streaming.check-interval'='4',       |
|   'flink.read.streaming.enabled'='true',           |
|   'flink.schema.0.data-type'='INT',                |
|   'flink.schema.0.name'='user_id',                 |
|   'flink.schema.1.data-type'='BIGINT',             |
|   'flink.schema.1.name'='item_id',                 |
|   'flink.schema.2.data-type'='INT',                |
|   'flink.schema.2.name'='category_id',             |
|   'flink.schema.3.data-type'='VARCHAR(2147483647)',  |
|   'flink.schema.3.name'='behavior',                |
|   'flink.schema.4.data-type'='BIGINT',             |
|   'flink.schema.4.name'='ts',                      |
|   'flink.schema.5.data-type'='VARCHAR(2147483647)',  |
|   'flink.schema.5.name'='dt',                      |
|   'flink.schema.6.data-type'='VARCHAR(2147483647)',  |
|   'flink.schema.6.name'='hh',                      |
|   'flink.schema.7.data-type'='VARCHAR(2147483647)',  |
|   'flink.schema.7.name'='mm',                      |
|   'flink.table.type'='MERGE_ON_READ',              |
|   'flink.write.precombine.field'='ts',             |
|   'flink.write.rate.limit'='2000',                 |
|   'flink.write.tasks'='1',                         |
|   'transient_lastDdlTime'='1655368376')            |
+----------------------------------------------------+


-- ================================================ user_behavior_hms_mor_tbl_ro ==============================================

-- INPUTFORMAT 是org.apache.hudi.hadoop.HoodieParquetInputFormat 这种方式只会查询出来parquet数据文件中的内容，但是刚刚更新或者删除的数据不能查出来


0: jdbc:hive2://hadoop102:10000> desc formatted user_behavior_hms_mor_tbl_ro;
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
| CreateTime:                   | Thu Jun 16 16:33:07 CST 2022                       | NULL                                               |
| LastAccessTime:               | UNKNOWN                                            | NULL                                               |
| Retention:                    | 0                                                  | NULL                                               |
| Location:                     | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl | NULL                                               |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                                               |
| Table Parameters:             | NULL                                               | NULL                                               |
|                               | EXTERNAL                                           | TRUE                                               |
|                               | last_commit_time_sync                              | 20220616163958182                                  |
|                               | numFiles                                           | 0                                                  |
|                               | numPartitions                                      | 11                                                 |
|                               | numRows                                            | 0                                                  |
|                               | rawDataSize                                        | 0                                                  |
|                               | spark.sql.sources.provider                         | hudi                                               |
|                               | spark.sql.sources.schema.numPartCols               | 3                                                  |
|                               | spark.sql.sources.schema.numParts                  | 1                                                  |
|                               | spark.sql.sources.schema.part.0                    | {\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"item_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"category_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"behavior\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hh\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"mm\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]} |
|                               | spark.sql.sources.schema.partCol.0                 | dt                                                 |
|                               | spark.sql.sources.schema.partCol.1                 | hh                                                 |
|                               | spark.sql.sources.schema.partCol.2                 | mm                                                 |
|                               | totalSize                                          | 0                                                  |
|                               | transient_lastDdlTime                              | 1655368387                                         |
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
|                               | hoodie.query.as.ro.table                           | true                                               |
|                               | path                                               | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl |
|                               | serialization.format                               | 1                                                  |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
0: jdbc:hive2://hadoop102:10000> show create table user_behavior_hms_mor_tbl_ro;
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `user_behavior_hms_mor_tbl_ro`( |
|   `_hoodie_commit_time` string COMMENT '''',         |
|   `_hoodie_commit_seqno` string COMMENT '''',        |
|   `_hoodie_record_key` string COMMENT '''',          |
|   `_hoodie_partition_path` string COMMENT '''',      |
|   `_hoodie_file_name` string COMMENT '''',           |
|   `user_id` int COMMENT '''',                        |
|   `item_id` bigint COMMENT '''',                     |
|   `category_id` int COMMENT '''',                    |
|   `behavior` string COMMENT '''',                    |
|   `ts` bigint COMMENT '''')                          |
| PARTITIONED BY (                                   |
|   `dt` string COMMENT '''',                          |
|   `hh` string COMMENT '''',                          |
|   `mm` string COMMENT '''')                          |
| ROW FORMAT SERDE                                   |
|   ''org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe''  |
| WITH SERDEPROPERTIES (                             |
|   ''hoodie.query.as.ro.table''=''true'',               |
|   ''path''=''hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl'')  |
| STORED AS INPUTFORMAT                              |
|   ''org.apache.hudi.hadoop.HoodieParquetInputFormat''  |
| OUTPUTFORMAT                                       |
|   ''org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'' |
| LOCATION                                           |
|   ''hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl'' |
| TBLPROPERTIES (                                    |
|   ''last_commit_time_sync''=''20220616164258050'',     |
|   ''spark.sql.sources.provider''=''hudi'',             |
|   ''spark.sql.sources.schema.numPartCols''=''3'',      |
|   ''spark.sql.sources.schema.numParts''=''1'',         |
|   ''spark.sql.sources.schema.part.0''=''{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"user_id","type":"integer","nullable":true,"metadata":{}},{"name":"item_id","type":"long","nullable":true,"metadata":{}},{"name":"category_id","type":"integer","nullable":true,"metadata":{}},{"name":"behavior","type":"string","nullable":true,"metadata":{}},{"name":"ts","type":"long","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"hh","type":"string","nullable":true,"metadata":{}},{"name":"mm","type":"string","nullable":true,"metadata":{}}]}'',  |
|   ''spark.sql.sources.schema.partCol.0''=''dt'',       |
|   ''spark.sql.sources.schema.partCol.1''=''hh'',       |
|   ''spark.sql.sources.schema.partCol.2''=''mm'',       |
|   ''transient_lastDdlTime''=''1655368387'')            |
+----------------------------------------------------+

-- ================================================ user_behavior_hms_mor_tbl_rt ==============================================

-- INPUTFORMAT 是 org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat 这种方式是能够实时读出来写入的数据，会将基于Parquet的基础列式文件、和基于行的Avro日志文件合并在一起呈现给用户

0: jdbc:hive2://hadoop102:10000> desc formatted user_behavior_hms_mor_tbl_rt;
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
| CreateTime:                   | Thu Jun 16 16:33:08 CST 2022                       | NULL                                               |
| LastAccessTime:               | UNKNOWN                                            | NULL                                               |
| Retention:                    | 0                                                  | NULL                                               |
| Location:                     | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl | NULL                                               |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                                               |
| Table Parameters:             | NULL                                               | NULL                                               |
|                               | EXTERNAL                                           | TRUE                                               |
|                               | last_commit_time_sync                              | 20220616164031029                                  |
|                               | numFiles                                           | 0                                                  |
|                               | numPartitions                                      | 12                                                 |
|                               | numRows                                            | 0                                                  |
|                               | rawDataSize                                        | 0                                                  |
|                               | spark.sql.sources.provider                         | hudi                                               |
|                               | spark.sql.sources.schema.numPartCols               | 3                                                  |
|                               | spark.sql.sources.schema.numParts                  | 1                                                  |
|                               | spark.sql.sources.schema.part.0                    | {\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"item_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"category_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"behavior\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hh\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"mm\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]} |
|                               | spark.sql.sources.schema.partCol.0                 | dt                                                 |
|                               | spark.sql.sources.schema.partCol.1                 | hh                                                 |
|                               | spark.sql.sources.schema.partCol.2                 | mm                                                 |
|                               | totalSize                                          | 0                                                  |
|                               | transient_lastDdlTime                              | 1655368388                                         |
|                               | NULL                                               | NULL                                               |
| # Storage Information         | NULL                                               | NULL                                               |
| SerDe Library:                | org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe | NULL                                               |
| InputFormat:                  | org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat | NULL                                               |
| OutputFormat:                 | org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat | NULL                                               |
| Compressed:                   | No                                                 | NULL                                               |
| Num Buckets:                  | 0                                                  | NULL                                               |
| Bucket Columns:               | []                                                 | NULL                                               |
| Sort Columns:                 | []                                                 | NULL                                               |
| Storage Desc Params:          | NULL                                               | NULL                                               |
|                               | hoodie.query.as.ro.table                           | false                                              |
|                               | path                                               | hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl |
|                               | serialization.format                               | 1                                                  |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
0: jdbc:hive2://hadoop102:10000> show create table user_behavior_hms_mor_tbl_rt;
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `user_behavior_hms_mor_tbl_rt`( |
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
|   'path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl')  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_mor_tbl' |
| TBLPROPERTIES (                                    |
|   'last_commit_time_sync'='20220616164055158',     |
|   'spark.sql.sources.provider'='hudi',             |
|   'spark.sql.sources.schema.numPartCols'='3',      |
|   'spark.sql.sources.schema.numParts'='1',         |
|   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"user_id","type":"integer","nullable":true,"metadata":{}},{"name":"item_id","type":"long","nullable":true,"metadata":{}},{"name":"category_id","type":"integer","nullable":true,"metadata":{}},{"name":"behavior","type":"string","nullable":true,"metadata":{}},{"name":"ts","type":"long","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"hh","type":"string","nullable":true,"metadata":{}},{"name":"mm","type":"string","nullable":true,"metadata":{}}]}',  |
|   'spark.sql.sources.schema.partCol.0'='dt',       |
|   'spark.sql.sources.schema.partCol.1'='hh',       |
|   'spark.sql.sources.schema.partCol.2'='mm',       |
|   'transient_lastDdlTime'='1655368388')            |
+----------------------------------------------------+























