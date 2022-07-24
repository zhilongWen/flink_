

-- https://zhuanlan.zhihu.com/p/471842018

-- hive 如何读 hudi 表 https://www.yuque.com/docs/share/879349ce-7de4-4284-9126-9c2a3c93a91d?#%20%E3%80%8AHive%20On%20Hudi%E3%80%8B

-- mysql table
CREATE TABLE IF NOT EXISTS orders_tbl
(
    order_id      INT            NOT NULL,
    order_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    customer_name VARCHAR(64)    NOT NULL,
    price         DECIMAL(10, 5) NOT NULL,
    product_id    INT,
    order_status  BOOLEAN,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (order_id)
)

INSERT INTO orders_tbl(order_id, customer_name, price, product_id, order_status)
VALUES (10001, 'u_1', 100.50, 360004, 1),
       (10002, 'u_2', 1.90, 360006, 0),
       (10003,  'u_3', 21.60, 360056, 1),
       (10004,  'u_4', 16.00, 360034, 1),
       (10005,  'u_1', 89.00, 360013, 0),
       (10006,  'u_2', 102.00, 360044, 1)


UPDATE `orders_tbl` SET price=1100.9 WHERE order_id=10002;
UPDATE `orders_tbl` SET price=0.9 WHERE order_id=10001;
UPDATE `orders_tbl` SET price=1.9 WHERE order_id=10002;
UPDATE `orders_tbl` SET price=10.0 WHERE order_id=10004;
UPDATE `orders_tbl` SET price=11.9 WHERE order_id=10005;
UPDATE `orders_tbl` SET price=90.0 WHERE order_id=10007;
UPDATE `orders_tbl` SET price=8.00 WHERE order_id=10003;
UPDATE `orders_tbl` SET price=56.00 WHERE order_id=10001;


-- ===============================================================================

create table if not exists source_order_tbl
(
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    order_id      int,
    order_date    TIMESTAMP(0),
    customer_name string,
    price         decimal(10, 5),
    product_id    int,
    order_status  boolean,
    ts TIMESTAMP(3),
    primary key (order_id) NOT ENFORCED,
    watermark for ts as ts - interval '1' second
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root',
    'connect.timeout' = '30s',
    'connect.max-retries' = '3',
    'connection.pool.size' = '5',
    'jdbc.properties.useSSL' = 'false',
    'jdbc.properties.characterEncoding' = 'utf-8',
    'database-name' = 'gmall_report',
    'table-name' = 'orders_tbl',
    'server-time-zone'='Asia/Shanghai',
    'debezium.snapshot.mode'='initial'
)

-- 要设置execution.checkpointing.interval开启checkpoint，只有checkpoint开启时才会commit数据到hdfs，这时数据才可见。测试时可以设置较少的时间间隔以便于数据观察，线上设置应该根据实际情况设定，设置的间隔不宜过小
create table if not exists order_cdc_hms_mor(
    order_id      int,
    order_date    string,
    customer_name string,
    price         double,
    product_id    int,
    order_status  int,
    ts             bigint,
    PRIMARY KEY (`order_id`) NOT ENFORCED
)with(
    'connector'='hudi',
    'path'='hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl', -- path为落地到hdfs的目录路径
    -- COPY_ON_WRITE：数据保存在列式文件中，如parquet。更新时可以定义数据版本或直接重写对应的parquet文件。支持快照读取和增量读取
    -- MERGE_ON_READ：数据保存在列式文件（如parquet) + 行记录级文件（如avro）中。数据更新时，会先将数据写到增量文件，然后会定时同步或异步合并成新的列式文件。支持快照读取和增量读取与读查询优化
    'table.type'='MERGE_ON_READ',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出
    'hoodie.datasource.write.recordkey.field' = 'order_id',  -- 主键 hoodie.datasource.write.recordkey.field为表去重主键，hudi根据这个配置创建数据索引，实现数据去重和增删改。主键相同时，选取write.precombine.field中对应字段的最大值的记录
    'write.precombine.field'= 'ts', -- 合并字段
    -- 写
    'write.tasks'='1',
    'write.bucket_assign.tasks'='1',
    'write.task.max.size'='1024',
    'write.rate.limit'='10000',
    -- 压缩
    'compaction.tasks'='1',
    'compaction.async.enabled'='true',
    'compaction.delta_commits'='1',
    'compaction.max_memory'='500',
    -- stream mode
    'read.streaming.enabled'='true',
    'read.streaming.check.interval'='3',
    -- hive
    'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc
    'hive_sync.enable'='true',           -- required，开启hive同步功能
    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,
    'hive_sync.table'='order_cdc_hms_mor_tbl',                          -- required, hive 新建的表名
    'hive_sync.db'='default',                       -- required, hive 新建的数据库名
    'hive_sync.support_timestamp'= 'true'
)


insert into order_cdc_hms_mor
select
    order_id,
    date_format(CAST(order_date AS STRING),'yyyy-MM-dd HH:mm:dd') order_date,
    customer_name,
    price,
    product_id,
    if(true,1,0) order_status,
    UNIX_TIMESTAMP(cast(ts as string),'yyyy-MM-dd HH:mm:dd') ts
from source_order_tbl


-- ====================================================================
-- hive table



-- ==================================   order_cdc_hms_mor_tbl_ro   ==================================


0: jdbc:hive2://hadoop102:10000> desc formatted order_cdc_hms_mor_tbl_ro;
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
|           col_name            |                     data_type                      |                      comment                       |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
| # col_name                    | data_type                                          | comment                                            |
| _hoodie_commit_time           | string                                             |                                                    |
| _hoodie_commit_seqno          | string                                             |                                                    |
| _hoodie_record_key            | string                                             |                                                    |
| _hoodie_partition_path        | string                                             |                                                    |
| _hoodie_file_name             | string                                             |                                                    |
| order_id                      | int                                                |                                                    |
| order_date                    | string                                             |                                                    |
| customer_name                 | string                                             |                                                    |
| price                         | double                                             |                                                    |
| product_id                    | int                                                |                                                    |
| order_status                  | int                                                |                                                    |
| ts                            | bigint                                             |                                                    |
|                               | NULL                                               | NULL                                               |
| # Detailed Table Information  | NULL                                               | NULL                                               |
| Database:                     | default                                            | NULL                                               |
| OwnerType:                    | USER                                               | NULL                                               |
| Owner:                        | zero                                               | NULL                                               |
| CreateTime:                   | Thu Jun 16 21:50:13 CST 2022                       | NULL                                               |
| LastAccessTime:               | UNKNOWN                                            | NULL                                               |
| Retention:                    | 0                                                  | NULL                                               |
| Location:                     | hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl | NULL                                               |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                                               |
| Table Parameters:             | NULL                                               | NULL                                               |
|                               | EXTERNAL                                           | TRUE                                               |
|                               | last_commit_time_sync                              | 20220616215010767                                  |
|                               | numFiles                                           | 1                                                  |
|                               | spark.sql.sources.provider                         | hudi                                               |
|                               | spark.sql.sources.schema.numParts                  | 1                                                  |
|                               | spark.sql.sources.schema.part.0                    | {\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"order_id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"order_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"price\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"product_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"order_status\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]} |
|                               | totalSize                                          | 436197                                             |
|                               | transient_lastDdlTime                              | 1655387413                                         |
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
|                               | path                                               | hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl |
|                               | serialization.format                               | 1                                                  |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
45 rows selected (0.041 seconds)
0: jdbc:hive2://hadoop102:10000> show create table order_cdc_hms_mor_tbl_ro;
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `order_cdc_hms_mor_tbl_ro`(  |
|   `_hoodie_commit_time` string COMMENT '',         |
|   `_hoodie_commit_seqno` string COMMENT '',        |
|   `_hoodie_record_key` string COMMENT '',          |
|   `_hoodie_partition_path` string COMMENT '',      |
|   `_hoodie_file_name` string COMMENT '',           |
|   `order_id` int COMMENT '',                       |
|   `order_date` string COMMENT '',                  |
|   `customer_name` string COMMENT '',               |
|   `price` double COMMENT '',                       |
|   `product_id` int COMMENT '',                     |
|   `order_status` int COMMENT '',                   |
|   `ts` bigint COMMENT '')                          |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
| WITH SERDEPROPERTIES (                             |
|   'hoodie.query.as.ro.table'='true',               |
|   'path'='hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl')  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hudi.hadoop.HoodieParquetInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl' |
| TBLPROPERTIES (                                    |
|   'last_commit_time_sync'='20220616215010767',     |
|   'spark.sql.sources.provider'='hudi',             |
|   'spark.sql.sources.schema.numParts'='1',         |
|   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"order_id","type":"integer","nullable":false,"metadata":{}},{"name":"order_date","type":"string","nullable":true,"metadata":{}},{"name":"customer_name","type":"string","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}},{"name":"product_id","type":"integer","nullable":true,"metadata":{}},{"name":"order_status","type":"integer","nullable":true,"metadata":{}},{"name":"ts","type":"long","nullable":true,"metadata":{}}]}',  |
|   'transient_lastDdlTime'='1655387413')            |
+----------------------------------------------------+
30 rows selected (0.029 seconds)
0: jdbc:hive2://hadoop102:10000>


-- ==================================   order_cdc_hms_mor_tbl_rt   ==================================


30 rows selected (0.029 seconds)
0: jdbc:hive2://hadoop102:10000> desc formatted order_cdc_hms_mor_tbl_rt;
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
|           col_name            |                     data_type                      |                      comment                       |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
| # col_name                    | data_type                                          | comment                                            |
| _hoodie_commit_time           | string                                             |                                                    |
| _hoodie_commit_seqno          | string                                             |                                                    |
| _hoodie_record_key            | string                                             |                                                    |
| _hoodie_partition_path        | string                                             |                                                    |
| _hoodie_file_name             | string                                             |                                                    |
| order_id                      | int                                                |                                                    |
| order_date                    | string                                             |                                                    |
| customer_name                 | string                                             |                                                    |
| price                         | double                                             |                                                    |
| product_id                    | int                                                |                                                    |
| order_status                  | int                                                |                                                    |
| ts                            | bigint                                             |                                                    |
|                               | NULL                                               | NULL                                               |
| # Detailed Table Information  | NULL                                               | NULL                                               |
| Database:                     | default                                            | NULL                                               |
| OwnerType:                    | USER                                               | NULL                                               |
| Owner:                        | zero                                               | NULL                                               |
| CreateTime:                   | Thu Jun 16 21:50:13 CST 2022                       | NULL                                               |
| LastAccessTime:               | UNKNOWN                                            | NULL                                               |
| Retention:                    | 0                                                  | NULL                                               |
| Location:                     | hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl | NULL                                               |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                                               |
| Table Parameters:             | NULL                                               | NULL                                               |
|                               | EXTERNAL                                           | TRUE                                               |
|                               | last_commit_time_sync                              | 20220616215010767                                  |
|                               | numFiles                                           | 1                                                  |
|                               | spark.sql.sources.provider                         | hudi                                               |
|                               | spark.sql.sources.schema.numParts                  | 1                                                  |
|                               | spark.sql.sources.schema.part.0                    | {\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"order_id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"order_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"price\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"product_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"order_status\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]} |
|                               | totalSize                                          | 436197                                             |
|                               | transient_lastDdlTime                              | 1655387413                                         |
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
|                               | path                                               | hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl |
|                               | serialization.format                               | 1                                                  |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
45 rows selected (0.039 seconds)
0: jdbc:hive2://hadoop102:10000> show create table order_cdc_hms_mor_tbl_rt;
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `order_cdc_hms_mor_tbl_rt`(  |
|   `_hoodie_commit_time` string COMMENT '',         |
|   `_hoodie_commit_seqno` string COMMENT '',        |
|   `_hoodie_record_key` string COMMENT '',          |
|   `_hoodie_partition_path` string COMMENT '',      |
|   `_hoodie_file_name` string COMMENT '',           |
|   `order_id` int COMMENT '',                       |
|   `order_date` string COMMENT '',                  |
|   `customer_name` string COMMENT '',               |
|   `price` double COMMENT '',                       |
|   `product_id` int COMMENT '',                     |
|   `order_status` int COMMENT '',                   |
|   `ts` bigint COMMENT '')                          |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
| WITH SERDEPROPERTIES (                             |
|   'hoodie.query.as.ro.table'='false',              |
|   'path'='hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl')  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl' |
| TBLPROPERTIES (                                    |
|   'last_commit_time_sync'='20220616215010767',     |
|   'spark.sql.sources.provider'='hudi',             |
|   'spark.sql.sources.schema.numParts'='1',         |
|   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"order_id","type":"integer","nullable":false,"metadata":{}},{"name":"order_date","type":"string","nullable":true,"metadata":{}},{"name":"customer_name","type":"string","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}},{"name":"product_id","type":"integer","nullable":true,"metadata":{}},{"name":"order_status","type":"integer","nullable":true,"metadata":{}},{"name":"ts","type":"long","nullable":true,"metadata":{}}]}',  |
|   'transient_lastDdlTime'='1655387413')            |
+----------------------------------------------------+
30 rows selected (0.034 seconds)
0: jdbc:hive2://hadoop102:10000>





0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_tbl_ro;
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
| order_cdc_hms_mor_tbl_ro._hoodie_commit_time  | order_cdc_hms_mor_tbl_ro._hoodie_commit_seqno  | order_cdc_hms_mor_tbl_ro._hoodie_record_key  | order_cdc_hms_mor_tbl_ro._hoodie_partition_path  |     order_cdc_hms_mor_tbl_ro._hoodie_file_name     | order_cdc_hms_mor_tbl_ro.order_id  | order_cdc_hms_mor_tbl_ro.order_date  | order_cdc_hms_mor_tbl_ro.customer_name  | order_cdc_hms_mor_tbl_ro.price  | order_cdc_hms_mor_tbl_ro.product_id  | order_cdc_hms_mor_tbl_ro.order_status  | order_cdc_hms_mor_tbl_ro.ts  |
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
| 20220616215000019                             | 20220616215000019_0_9                          | 10011                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10011                              | 2022-06-15 01:47:15                  | u_1                                     | 9.5                             | 360014                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_15                         | 10002                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10002                              | 2022-06-15 01:15:15                  | u_2                                     | 1.9                             | 360006                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_7                          | 10013                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10013                              | 2022-06-15 01:47:15                  | u_5                                     | 63.6                            | 360078                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_19                         | 10001                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10001                              | 2022-06-15 01:15:15                  | u_1                                     | 56.0                            | 360004                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_10                         | 10012                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10012                              | 2022-06-15 01:47:15                  | u_21                                    | 19.9                            | 360009                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_16                         | 10004                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10004                              | 2022-06-15 01:15:15                  | u_4                                     | 10.0                            | 360034                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_5                          | 10015                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10015                              | 2022-06-15 01:47:15                  | u_7                                     | 80.5                            | 360009                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_18                         | 10003                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10003                              | 2022-06-15 01:15:15                  | u_3                                     | 8.0                             | 360056                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_8                          | 10014                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10014                              | 2022-06-15 01:47:15                  | u_3                                     | 11.0                            | 360090                               | 1                                      | 1654710420                   |
| 20220616215000019                             | 20220616215000019_0_12                         | 10006                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10006                              | 2022-06-15 01:15:15                  | u_2                                     | 102.0                           | 360044                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_6                          | 10016                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10016                              | 2022-06-15 01:47:15                  | u_9                                     | 10.6                            | 360056                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_17                         | 10005                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215240246.parquet | 10005                              | 2022-06-15 01:15:15                  | u_1                                     | 11.9                            | 360013                               | 1                                      | 1656090900                   |
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
12 rows selected (0.093 seconds)



0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_tbl_ro;
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
| order_cdc_hms_mor_tbl_ro._hoodie_commit_time  | order_cdc_hms_mor_tbl_ro._hoodie_commit_seqno  | order_cdc_hms_mor_tbl_ro._hoodie_record_key  | order_cdc_hms_mor_tbl_ro._hoodie_partition_path  |     order_cdc_hms_mor_tbl_ro._hoodie_file_name     | order_cdc_hms_mor_tbl_ro.order_id  | order_cdc_hms_mor_tbl_ro.order_date  | order_cdc_hms_mor_tbl_ro.customer_name  | order_cdc_hms_mor_tbl_ro.price  | order_cdc_hms_mor_tbl_ro.product_id  | order_cdc_hms_mor_tbl_ro.order_status  | order_cdc_hms_mor_tbl_ro.ts  |
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
| 20220616215000019                             | 20220616215000019_0_9                          | 10011                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10011                              | 2022-06-15 01:47:15                  | u_1                                     | 9.5                             | 360014                               | 1                                      | 1654710420                   |
| 20220616215240283                             | 20220616215240283_0_20                         | 10002                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10002                              | 2022-06-15 01:15:15                  | u_2                                     | 1100.9                          | 360006                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_7                          | 10013                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10013                              | 2022-06-15 01:47:15                  | u_5                                     | 63.6                            | 360078                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_19                         | 10001                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10001                              | 2022-06-15 01:15:15                  | u_1                                     | 56.0                            | 360004                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_10                         | 10012                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10012                              | 2022-06-15 01:47:15                  | u_21                                    | 19.9                            | 360009                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_16                         | 10004                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10004                              | 2022-06-15 01:15:15                  | u_4                                     | 10.0                            | 360034                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_5                          | 10015                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10015                              | 2022-06-15 01:47:15                  | u_7                                     | 80.5                            | 360009                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_18                         | 10003                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10003                              | 2022-06-15 01:15:15                  | u_3                                     | 8.0                             | 360056                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_8                          | 10014                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10014                              | 2022-06-15 01:47:15                  | u_3                                     | 11.0                            | 360090                               | 1                                      | 1654710420                   |
| 20220616215000019                             | 20220616215000019_0_12                         | 10006                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10006                              | 2022-06-15 01:15:15                  | u_2                                     | 102.0                           | 360044                               | 1                                      | 1656090900                   |
| 20220616215000019                             | 20220616215000019_0_6                          | 10016                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10016                              | 2022-06-15 01:47:15                  | u_9                                     | 10.6                            | 360056                               | 1                                      | 1654710420                   |
| 20220616215010767                             | 20220616215010767_0_17                         | 10005                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616215900292.parquet | 10005                              | 2022-06-15 01:15:15                  | u_1                                     | 11.9                            | 360013                               | 1                                      | 1656090900                   |
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
12 rows selected (0.075 seconds)
0: jdbc:hive2://hadoop102:10000>



set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat; -- 这地方指定为HoodieCombineHiveInputFormat
set hoodie.hudimor.consume.mode=INCREMENTAL;
set hoodie.hudimor.consume.max.commits=-1;
set hoodie.hudimor.consume.start.timestamp=20220616215010767;
select * from order_cdc_hms_mor_tbl_ro where order_id = 10002 and `_hoodie_commit_time` > '20220616215010767';
0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_tbl_ro where order_id = 10002 and `_hoodie_commit_time` > '20220616215010767';
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
| order_cdc_hms_mor_tbl_ro._hoodie_commit_time  | order_cdc_hms_mor_tbl_ro._hoodie_commit_seqno  | order_cdc_hms_mor_tbl_ro._hoodie_record_key  | order_cdc_hms_mor_tbl_ro._hoodie_partition_path  |     order_cdc_hms_mor_tbl_ro._hoodie_file_name     | order_cdc_hms_mor_tbl_ro.order_id  | order_cdc_hms_mor_tbl_ro.order_date  | order_cdc_hms_mor_tbl_ro.customer_name  | order_cdc_hms_mor_tbl_ro.price  | order_cdc_hms_mor_tbl_ro.product_id  | order_cdc_hms_mor_tbl_ro.order_status  | order_cdc_hms_mor_tbl_ro.ts  |
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
| 20220616215900358                             | 20220616215900358_0_21                         | 10002                                        |                                                  | 08b564d4-7667-489d-9a1e-868843c1cfb6_0-1-0_20220616220640232.parquet | 10002                              | 2022-06-15 01:15:15                  | u_2                                     | 20.9                            | 360006                               | 1                                      | 1656090900                   |
+-----------------------------------------------+------------------------------------------------+----------------------------------------------+--------------------------------------------------+----------------------------------------------------+------------------------------------+--------------------------------------+-----------------------------------------+---------------------------------+--------------------------------------+----------------------------------------+------------------------------+
1 row selected (0.086 seconds)


set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat; -- 这地方指定为HoodieCombineHiveInputFormat
set hoodie.hudimor.consume.mode=INCREMENTAL;
set hoodie.hudimor.consume.max.commits=-1;
set hoodie.hudimor.consume.start.timestamp=20220616215010767;
select * from order_cdc_hms_mor_tbl_ro where order_id = 10002 and `_hoodie_commit_time` = '20220616215010767';





