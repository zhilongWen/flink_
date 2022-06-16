
-- https://zhuanlan.zhihu.com/p/471842018
-- https://www.yuque.com/docs/share/879349ce-7de4-4284-9126-9c2a3c93a91d?#D8kib
-- https://www.yuque.com/yuzhao-my9fz/kb/kgv2rb

-- mysql
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

INSERT INTO orders_tbl(order_id, customer_name, price, product_id, order_status)
VALUES (10011, 'u_1', 9.50, 360014, 1),
       (10012, 'u_21', 19.90, 360009, 0),
       (10013,  'u_5', 63.60, 360078, 1),
       (10014,  'u_3', 11.00, 360090, 1),
       (10015,  'u_7', 80.50, 360009, 0),
       (10016,  'u_9', 10.60, 360056, 1)


-- ./bin/yarn-session.sh -nm flink-job -jm 1024m -tm 4096 -s 4 -qu hive -Dyarn.provided.lib.dirs='hdfs://hadoop102:8020/flink-remote-dir/lib' -d

-- ./bin/sql-client.sh -s yarn-session -j ./lib/*

-- load hive
create catalog myhive with('type'='hive','hive-conf-dir'='/opt/module/hive-3.1.2/conf','default-database'='default','hive-version'='3.1.2','hadoop-conf-dir'='/opt/module/hadoop-3.1.3/etc/hadoop');

use catalog myhive;

load module hive;

use modules hive,core;

-- init env
SET 'execution.runtime-mode' = 'streaming';

SET 'execution.checkpointing.interval' = '10s';

SET 'sql-client.execution.result-mode' = 'tableau';


-- ========================================

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
create table if not exists order_cdc_hms_mor_sql(
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
    'path'='hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_sql_tbl', -- path为落地到hdfs的目录路径
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
    'hive_sync.table'='order_cdc_hms_mor_sql_tbl',                          -- required, hive 新建的表名
    'hive_sync.db'='default',                       -- required, hive 新建的数据库名
    'hive_sync.support_timestamp'= 'true'
)


insert into order_cdc_hms_mor_sql
select
    order_id,
    date_format(CAST(order_date AS STRING),'yyyy-MM-dd HH:mm:dd') order_date,
    customer_name,
    price,
    product_id,
    if(true,1,0) order_status,
    UNIX_TIMESTAMP(cast(ts as string),'yyyy-MM-dd HH:mm:dd') ts
from source_order_tbl


-- ==============================================================  FLINK CLIENT

         Flink SQL> show tables;
+------------------------------+
|                   table name |
+------------------------------+
|               file_soure_tbl |
|       hudi_user_behavior_tbl |
|            order_cdc_hms_mor |
|     order_cdc_hms_mor_tbl_ro |
|     order_cdc_hms_mor_tbl_rt |
|             source_order_tbl |
|        user_behavior_hms_cow |
|    user_behavior_hms_cow_tbl |
|        user_behavior_hms_mor |
| user_behavior_hms_mor_tbl_ro |
| user_behavior_hms_mor_tbl_rt |
|       user_behavior_jdbc_mor |
|            user_behavior_tbl |
+------------------------------+
13 rows in set

    Flink SQL> create table if not exists source_order_tbl
> (
>     db_name STRING METADATA FROM 'database_name' VIRTUAL,
>     table_name STRING METADATA  FROM 'table_name' VIRTUAL,
>     operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
>     order_id      int,
>     order_date    TIMESTAMP(0),
>     customer_name string,
>     price         decimal(10, 5),
>     product_id    int,
>     order_status  boolean,
>     ts TIMESTAMP(3),
>     primary key (order_id) NOT ENFORCED,
>     watermark for ts as ts - interval '1' second
> ) WITH (
>     'connector' = 'mysql-cdc',
>     'hostname' = 'hadoop102',
>     'port' = '3306',
>     'username' = 'root',
>     'password' = 'root',
>     'connect.timeout' = '30s',
>     'connect.max-retries' = '3',
>     'connection.pool.size' = '5',
>     'jdbc.properties.useSSL' = 'false',
>     'jdbc.properties.characterEncoding' = 'utf-8',
>     'database-name' = 'gmall_report',
>     'table-name' = 'orders_tbl',
>     'server-time-zone'='Asia/Shanghai',
>     'debezium.snapshot.mode'='initial'
> );
[INFO] Execute statement succeed.

Flink SQL> select * from source_order_tbl;
2022-06-16 23:03:50,339 WARN  org.apache.flink.yarn.configuration.YarnLogConfigUtil        [] - The configuration directory ('/opt/module/flink/conf') already contains a LOG4J config file.If you want to use logback, then please delete or rename the log configuration file.
2022-06-16 23:03:50,434 INFO  org.apache.hadoop.yarn.client.RMProxy                        [] - Connecting to ResourceManager at hadoop103/192.168.170.103:8032
2022-06-16 23:03:50,513 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
2022-06-16 23:03:50,558 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Found Web Interface hadoop104:36273 of application 'application_1655374284508_0002'.
+----+--------------------------------+--------------------------------+-------------------------+-------------+---------------------+--------------------------------+--------------+-------------+--------------+-------------------------+
| op |                        db_name |                     table_name |            operation_ts |    order_id |          order_date |                  customer_name |        price |  product_id | order_status |                      ts |
+----+--------------------------------+--------------------------------+-------------------------+-------------+---------------------+--------------------------------+--------------+-------------+--------------+-------------------------+
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10013 | 2022-06-15 01:47:09 |                            u_5 |     63.60000 |      360078 |         true | 2022-06-15 01:47:09.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10012 | 2022-06-15 01:47:09 |                           u_21 |     19.90000 |      360009 |        false | 2022-06-15 01:47:09.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10011 | 2022-06-15 01:47:09 |                            u_1 |      9.50000 |      360014 |         true | 2022-06-15 01:47:09.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10001 | 2022-06-15 01:15:25 |                            u_1 |     56.00000 |      360004 |         true | 2022-06-15 01:15:25.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10016 | 2022-06-15 01:47:09 |                            u_9 |     10.60000 |      360056 |         true | 2022-06-15 01:47:09.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10015 | 2022-06-15 01:47:09 |                            u_7 |     80.50000 |      360009 |        false | 2022-06-15 01:47:09.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10014 | 2022-06-15 01:47:09 |                            u_3 |     11.00000 |      360090 |         true | 2022-06-15 01:47:09.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10005 | 2022-06-15 01:15:25 |                            u_1 |     11.90000 |      360013 |        false | 2022-06-15 01:15:25.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10004 | 2022-06-15 01:15:25 |                            u_4 |     10.00000 |      360034 |         true | 2022-06-15 01:15:25.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10003 | 2022-06-15 01:15:25 |                            u_3 |      8.00000 |      360056 |         true | 2022-06-15 01:15:25.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10002 | 2022-06-15 01:15:25 |                            u_2 |      8.00000 |      360006 |        false | 2022-06-15 01:15:25.000 |
| +I |                   gmall_report |                     orders_tbl | 1970-01-01 08:00:00.000 |       10006 | 2022-06-15 01:15:25 |                            u_2 |    102.00000 |      360044 |         true | 2022-06-15 01:15:25.000 |
^CQuery terminated, received a total of 12 rows

Flink SQL> -- 要设置execution.checkpointing.interval开启checkpoint，只有checkpoint开启时才会commit数据到hdfs，这时数据才可见。测试时可以设置较少的时间间隔以便于数据观察，线上设置应该根据 .际情况设定，设置的间隔不宜过小
> create table if not exists order_cdc_hms_mor_sql(
    >     order_id      int,
    >     order_date    string,
    >     customer_name string,
    >     price         double,
    >     product_id    int,
    >     order_status  int,
    >     ts             bigint,
    >     PRIMARY KEY (`order_id`) NOT ENFORCED
    > )with(
    >     'connector'='hudi',
    >     'path'='hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_sql_tbl', -- path为落地到hdfs的目录路径
    >     -- COPY_ON_WRITE：数据保存在列式文件中，如parquet。更新时可以定义数据版本或直接重写对应的parquet文件。支持快照读取和增量读取
    >     -- MERGE_ON_READ：数据保存在列式文件（如parquet) + 行记录级文件（如avro）中。数据更新时，会先将数据写到增量文件，然后会定时同步或异步合并成新的列式文件。支持快照读取和增量读取与读查查.优化
    >     'table.type'='MERGE_ON_READ',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出
    >     'hoodie.datasource.write.recordkey.field' = 'order_id',  -- 主键 hoodie.datasource.write.recordkey.field为表去重主键，hudi根据这个配置创建数据索引，实现数据去重和增删改。主键相同时 .选取write.precombine.field中对应字段的最大值的记录
    >     'write.precombine.field'= 'ts', -- 合并字段
    >     -- 写
    >     'write.tasks'='1',
    >     'write.bucket_assign.tasks'='1',
    >     'write.task.max.size'='1024',
    >     'write.rate.limit'='10000',
    >     -- 压缩
    >     'compaction.tasks'='1',
    >     'compaction.async.enabled'='true',
    >     'compaction.delta_commits'='1',
    >     'compaction.max_memory'='500',
    >     -- stream mode
    >     'read.streaming.enabled'='true',
    >     'read.streaming.check.interval'='3',
    >     -- hive
    >     'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc
    >     'hive_sync.enable'='true',           -- required，开启hive同步功能
    >     'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,
    >     'hive_sync.table'='order_cdc_hms_mor_sql_tbl',                          -- required, hive 新建的表名
    >     'hive_sync.db'='default',                       -- required, hive 新建的数据库名
    >     'hive_sync.support_timestamp'= 'true'
    > );
[INFO] Execute statement succeed.

Flink SQL> insert into order_cdc_hms_mor_sql
                                        > select
                                                  >     order_id,
                                                  >     date_format(CAST(order_date AS STRING),'yyyy-MM-dd HH:mm:dd') order_date,
                                                  >     customer_name,
                                                  >     price,
                                                  >     product_id,
                                                  >     if(true,1,0) order_status,
                                                  >     UNIX_TIMESTAMP(cast(ts as string),'yyyy-MM-dd HH:mm:dd') ts
                                                  > from source_order_tbl
                                                  > ;
[INFO] Submitting SQL update statement to the cluster...
                          2022-06-16 23:05:29,023 INFO  org.apache.hadoop.yarn.client.RMProxy                        [] - Connecting to ResourceManager at hadoop103/192.168.170.103:8032
                          2022-06-16 23:05:29,024 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
                          2022-06-16 23:05:29,030 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Found Web Interface hadoop104:36273 of application 'application_1655374284508_0002'.
                          [INFO] SQL update statement has been successfully submitted to the cluster:
                                         Job ID: f91a87223735593aa1ba1374b87e51b3


                                         Flink SQL> show tables;
+------------------------------+
|                   table name |
+------------------------------+
|               file_soure_tbl |
|       hudi_user_behavior_tbl |
|            order_cdc_hms_mor |
|        order_cdc_hms_mor_sql |
| order_cdc_hms_mor_sql_tbl_ro |
| order_cdc_hms_mor_sql_tbl_rt |
|     order_cdc_hms_mor_tbl_ro |
|     order_cdc_hms_mor_tbl_rt |
|             source_order_tbl |
|        user_behavior_hms_cow |
|    user_behavior_hms_cow_tbl |
|        user_behavior_hms_mor |
| user_behavior_hms_mor_tbl_ro |
| user_behavior_hms_mor_tbl_rt |
|       user_behavior_jdbc_mor |
|            user_behavior_tbl |
+------------------------------+
16 rows in set

    Flink SQL> show tables;
+------------------------------+
|                   table name |
+------------------------------+
|               file_soure_tbl |
|       hudi_user_behavior_tbl |
|            order_cdc_hms_mor |
|        order_cdc_hms_mor_sql |
| order_cdc_hms_mor_sql_tbl_ro |
| order_cdc_hms_mor_sql_tbl_rt |
|     order_cdc_hms_mor_tbl_ro |
|     order_cdc_hms_mor_tbl_rt |
|             source_order_tbl |
|        user_behavior_hms_cow |
|    user_behavior_hms_cow_tbl |
|        user_behavior_hms_mor |
| user_behavior_hms_mor_tbl_ro |
| user_behavior_hms_mor_tbl_rt |
|       user_behavior_jdbc_mor |
|            user_behavior_tbl |
+------------------------------+
16 rows in set

    Flink SQL> select * from order_cdc_hms_mor_sql_tbl_ro;
2022-06-16 23:07:04,471 INFO  org.apache.hadoop.yarn.client.RMProxy                        [] - Connecting to ResourceManager at hadoop103/192.168.170.103:8032
2022-06-16 23:07:04,472 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
2022-06-16 23:07:04,477 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Found Web Interface hadoop104:36273 of application 'application_1655374284508_0002'.
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
| op |            _hoodie_commit_time |           _hoodie_commit_seqno |             _hoodie_record_key |         _hoodie_partition_path |              _hoodie_file_name |    order_id |                     order_date |                  customer_name |                          price |  product_id | order_status |                   ts |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
| +I |              20220616230547849 |          20220616230547849_0_9 |                          10011 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10011 |            2022-06-15 01:47:15 |                            u_1 |                            9.5 |      360014 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_2 |                          10002 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10002 |            2022-06-15 01:15:15 |                            u_2 |                            8.0 |      360006 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_5 |                          10013 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10013 |            2022-06-15 01:47:15 |                            u_5 |                           63.6 |      360078 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_1 |                          10001 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10001 |            2022-06-15 01:15:15 |                            u_1 |                           56.0 |      360004 |            1 |           1656119700 |
| +I |              20220616230547849 |         20220616230547849_0_10 |                          10012 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10012 |            2022-06-15 01:47:15 |                           u_21 |                           19.9 |      360009 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_4 |                          10004 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10004 |            2022-06-15 01:15:15 |                            u_4 |                           10.0 |      360034 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_7 |                          10015 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10015 |            2022-06-15 01:47:15 |                            u_7 |                           80.5 |      360009 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_3 |                          10003 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10003 |            2022-06-15 01:15:15 |                            u_3 |                            8.0 |      360056 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_6 |                          10014 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10014 |            2022-06-15 01:47:15 |                            u_3 |                           11.0 |      360090 |            1 |           1654739220 |
| +I |              20220616230547849 |         20220616230547849_0_12 |                          10006 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10006 |            2022-06-15 01:15:15 |                            u_2 |                          102.0 |      360044 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_8 |                          10016 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10016 |            2022-06-15 01:47:15 |                            u_9 |                           10.6 |      360056 |            1 |           1654739220 |
| +I |              20220616230547849 |         20220616230547849_0_11 |                          10005 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10005 |            2022-06-15 01:15:15 |                            u_1 |                           11.9 |      360013 |            1 |           1656119700 |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
Received a total of 12 rows

Flink SQL> select * from order_cdc_hms_mor_sql_tbl_rt;
2022-06-16 23:07:22,736 INFO  org.apache.hadoop.yarn.client.RMProxy                        [] - Connecting to ResourceManager at hadoop103/192.168.170.103:8032
2022-06-16 23:07:22,736 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
2022-06-16 23:07:22,741 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Found Web Interface hadoop104:36273 of application 'application_1655374284508_0002'.
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
| op |            _hoodie_commit_time |           _hoodie_commit_seqno |             _hoodie_record_key |         _hoodie_partition_path |              _hoodie_file_name |    order_id |                     order_date |                  customer_name |                          price |  product_id | order_status |                   ts |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
| +I |              20220616230547849 |          20220616230547849_0_9 |                          10011 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10011 |            2022-06-15 01:47:15 |                            u_1 |                            9.5 |      360014 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_2 |                          10002 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10002 |            2022-06-15 01:15:15 |                            u_2 |                            8.0 |      360006 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_5 |                          10013 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10013 |            2022-06-15 01:47:15 |                            u_5 |                           63.6 |      360078 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_1 |                          10001 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10001 |            2022-06-15 01:15:15 |                            u_1 |                           56.0 |      360004 |            1 |           1656119700 |
| +I |              20220616230547849 |         20220616230547849_0_10 |                          10012 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10012 |            2022-06-15 01:47:15 |                           u_21 |                           19.9 |      360009 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_4 |                          10004 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10004 |            2022-06-15 01:15:15 |                            u_4 |                           10.0 |      360034 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_7 |                          10015 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10015 |            2022-06-15 01:47:15 |                            u_7 |                           80.5 |      360009 |            1 |           1654739220 |
| +I |              20220616230547849 |          20220616230547849_0_3 |                          10003 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10003 |            2022-06-15 01:15:15 |                            u_3 |                            8.0 |      360056 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_6 |                          10014 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10014 |            2022-06-15 01:47:15 |                            u_3 |                           11.0 |      360090 |            1 |           1654739220 |
| +I |              20220616230547849 |         20220616230547849_0_12 |                          10006 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10006 |            2022-06-15 01:15:15 |                            u_2 |                          102.0 |      360044 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_8 |                          10016 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10016 |            2022-06-15 01:47:15 |                            u_9 |                           10.6 |      360056 |            1 |           1654739220 |
| +I |              20220616230547849 |         20220616230547849_0_11 |                          10005 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10005 |            2022-06-15 01:15:15 |                            u_1 |                           11.9 |      360013 |            1 |           1656119700 |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
Received a total of 12 rows

Flink SQL> select * from order_cdc_hms_mor_sql_tbl_rt;
2022-06-16 23:09:11,548 INFO  org.apache.hadoop.yarn.client.RMProxy                        [] - Connecting to ResourceManager at hadoop103/192.168.170.103:8032
2022-06-16 23:09:11,548 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
2022-06-16 23:09:11,554 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Found Web Interface hadoop104:36273 of application 'application_1655374284508_0002'.
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
| op |            _hoodie_commit_time |           _hoodie_commit_seqno |             _hoodie_record_key |         _hoodie_partition_path |              _hoodie_file_name |    order_id |                     order_date |                  customer_name |                          price |  product_id | order_status |                   ts |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
| +I |              20220616230547849 |          20220616230547849_0_9 |                          10011 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10011 |            2022-06-15 01:47:15 |                            u_1 |                            9.5 |      360014 |            1 |           1654739220 |
| +I |              20220616230553618 |         20220616230553618_0_15 |                          10002 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10002 |            2022-06-15 01:15:15 |                            u_2 |                           10.0 |      360006 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_5 |                          10013 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10013 |            2022-06-15 01:47:15 |                            u_5 |                           63.6 |      360078 |            1 |           1654739220 |
| +I |              20220616230553618 |         20220616230553618_0_19 |                          10001 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10001 |            2022-06-15 01:15:15 |                            u_1 |                          560.0 |      360004 |            1 |           1656119700 |
| +I |              20220616230547849 |         20220616230547849_0_10 |                          10012 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10012 |            2022-06-15 01:47:15 |                           u_21 |                           19.9 |      360009 |            1 |           1654739220 |
| +I |              20220616230553618 |         20220616230553618_0_16 |                          10004 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10004 |            2022-06-15 01:15:15 |                            u_4 |                           12.9 |      360034 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_7 |                          10015 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10015 |            2022-06-15 01:47:15 |                            u_7 |                           80.5 |      360009 |            1 |           1654739220 |
| +I |              20220616230553618 |         20220616230553618_0_18 |                          10003 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10003 |            2022-06-15 01:15:15 |                            u_3 |                            9.1 |      360056 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_6 |                          10014 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10014 |            2022-06-15 01:47:15 |                            u_3 |                           11.0 |      360090 |            1 |           1654739220 |
| +I |              20220616230547849 |         20220616230547849_0_12 |                          10006 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10006 |            2022-06-15 01:15:15 |                            u_2 |                          102.0 |      360044 |            1 |           1656119700 |
| +I |              20220616230547849 |          20220616230547849_0_8 |                          10016 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10016 |            2022-06-15 01:47:15 |                            u_9 |                           10.6 |      360056 |            1 |           1654739220 |
| +I |              20220616230553618 |         20220616230553618_0_17 |                          10005 |                                | 4d6e4f9a-43eb-4468-9ace-64f... |       10005 |            2022-06-15 01:15:15 |                            u_1 |                           13.9 |      360013 |            1 |           1656119700 |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+--------------------------------+-------------+--------------+----------------------+
Received a total of 12 rows

Flink SQL>

-- ==============================================================  HIVE CLIENT
0: jdbc:hive2://hadoop102:10000> show tables;
+-------------------------------+
|           tab_name            |
+-------------------------------+
| file_soure_tbl                |
| hudi_user_behavior_tbl        |
| order_cdc_hms_mor             |
| order_cdc_hms_mor_sql         |
| order_cdc_hms_mor_sql_tbl_ro  |
| order_cdc_hms_mor_sql_tbl_rt  |
| order_cdc_hms_mor_tbl_ro      |
| order_cdc_hms_mor_tbl_rt      |
| source_order_tbl              |
| user_behavior_hms_cow         |
| user_behavior_hms_cow_tbl     |
| user_behavior_hms_mor         |
| user_behavior_hms_mor_tbl_ro  |
| user_behavior_hms_mor_tbl_rt  |
| user_behavior_jdbc_mor        |
| user_behavior_tbl             |
+-------------------------------+
16 rows selected (0.026 seconds)
0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_sql_tbl_ro;
+---------------------------------------------------+----------------------------------------------------+--------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------+------------------------------------------+---------------------------------------------+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------+
| order_cdc_hms_mor_sql_tbl_ro._hoodie_commit_time  | order_cdc_hms_mor_sql_tbl_ro._hoodie_commit_seqno  | order_cdc_hms_mor_sql_tbl_ro._hoodie_record_key  | order_cdc_hms_mor_sql_tbl_ro._hoodie_partition_path |   order_cdc_hms_mor_sql_tbl_ro._hoodie_file_name   | order_cdc_hms_mor_sql_tbl_ro.order_id  | order_cdc_hms_mor_sql_tbl_ro.order_date  | order_cdc_hms_mor_sql_tbl_ro.customer_name  | order_cdc_hms_mor_sql_tbl_ro.price  | order_cdc_hms_mor_sql_tbl_ro.product_id  | order_cdc_hms_mor_sql_tbl_ro.order_status  | order_cdc_hms_mor_sql_tbl_ro.ts  |
+---------------------------------------------------+----------------------------------------------------+--------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------+------------------------------------------+---------------------------------------------+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------+
| 20220616230547849                                 | 20220616230547849_0_9                              | 10011                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10011                                  | 2022-06-15 01:47:15                      | u_1                                         | 9.5                                 | 360014                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_2                              | 10002                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10002                                  | 2022-06-15 01:15:15                      | u_2                                         | 8.0                                 | 360006                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_5                              | 10013                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10013                                  | 2022-06-15 01:47:15                      | u_5                                         | 63.6                                | 360078                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_1                              | 10001                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10001                                  | 2022-06-15 01:15:15                      | u_1                                         | 56.0                                | 360004                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_10                             | 10012                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10012                                  | 2022-06-15 01:47:15                      | u_21                                        | 19.9                                | 360009                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_4                              | 10004                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10004                                  | 2022-06-15 01:15:15                      | u_4                                         | 10.0                                | 360034                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_7                              | 10015                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10015                                  | 2022-06-15 01:47:15                      | u_7                                         | 80.5                                | 360009                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_3                              | 10003                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10003                                  | 2022-06-15 01:15:15                      | u_3                                         | 8.0                                 | 360056                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_6                              | 10014                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10014                                  | 2022-06-15 01:47:15                      | u_3                                         | 11.0                                | 360090                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_12                             | 10006                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10006                                  | 2022-06-15 01:15:15                      | u_2                                         | 102.0                               | 360044                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_8                              | 10016                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10016                                  | 2022-06-15 01:47:15                      | u_9                                         | 10.6                                | 360056                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_11                             | 10005                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230553294.parquet | 10005                                  | 2022-06-15 01:15:15                      | u_1                                         | 11.9                                | 360013                                   | 1                                          | 1656119700                       |
+---------------------------------------------------+----------------------------------------------------+--------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------+------------------------------------------+---------------------------------------------+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------+
12 rows selected (0.083 seconds)
0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_sql_tbl_rt;
Error: java.lang.NoClassDefFoundError: org/apache/parquet/schema/LogicalTypeAnnotation (state=,code=0)
0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_sql_tbl_rt;
Error: java.lang.NoClassDefFoundError: org/apache/parquet/schema/LogicalTypeAnnotation (state=,code=0)
0: jdbc:hive2://hadoop102:10000> select * from order_cdc_hms_mor_sql_tbl_ro;
+---------------------------------------------------+----------------------------------------------------+--------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------+------------------------------------------+---------------------------------------------+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------+
| order_cdc_hms_mor_sql_tbl_ro._hoodie_commit_time  | order_cdc_hms_mor_sql_tbl_ro._hoodie_commit_seqno  | order_cdc_hms_mor_sql_tbl_ro._hoodie_record_key  | order_cdc_hms_mor_sql_tbl_ro._hoodie_partition_path |   order_cdc_hms_mor_sql_tbl_ro._hoodie_file_name   | order_cdc_hms_mor_sql_tbl_ro.order_id  | order_cdc_hms_mor_sql_tbl_ro.order_date  | order_cdc_hms_mor_sql_tbl_ro.customer_name  | order_cdc_hms_mor_sql_tbl_ro.price  | order_cdc_hms_mor_sql_tbl_ro.product_id  | order_cdc_hms_mor_sql_tbl_ro.order_status  | order_cdc_hms_mor_sql_tbl_ro.ts  |
+---------------------------------------------------+----------------------------------------------------+--------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------+------------------------------------------+---------------------------------------------+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------+
| 20220616230547849                                 | 20220616230547849_0_9                              | 10011                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10011                                  | 2022-06-15 01:47:15                      | u_1                                         | 9.5                                 | 360014                                   | 1                                          | 1654739220                       |
| 20220616230553618                                 | 20220616230553618_0_15                             | 10002                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10002                                  | 2022-06-15 01:15:15                      | u_2                                         | 10.0                                | 360006                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_5                              | 10013                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10013                                  | 2022-06-15 01:47:15                      | u_5                                         | 63.6                                | 360078                                   | 1                                          | 1654739220                       |
| 20220616230553618                                 | 20220616230553618_0_19                             | 10001                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10001                                  | 2022-06-15 01:15:15                      | u_1                                         | 560.0                               | 360004                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_10                             | 10012                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10012                                  | 2022-06-15 01:47:15                      | u_21                                        | 19.9                                | 360009                                   | 1                                          | 1654739220                       |
| 20220616230553618                                 | 20220616230553618_0_16                             | 10004                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10004                                  | 2022-06-15 01:15:15                      | u_4                                         | 12.9                                | 360034                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_7                              | 10015                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10015                                  | 2022-06-15 01:47:15                      | u_7                                         | 80.5                                | 360009                                   | 1                                          | 1654739220                       |
| 20220616230553618                                 | 20220616230553618_0_18                             | 10003                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10003                                  | 2022-06-15 01:15:15                      | u_3                                         | 9.1                                 | 360056                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_6                              | 10014                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10014                                  | 2022-06-15 01:47:15                      | u_3                                         | 11.0                                | 360090                                   | 1                                          | 1654739220                       |
| 20220616230547849                                 | 20220616230547849_0_12                             | 10006                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10006                                  | 2022-06-15 01:15:15                      | u_2                                         | 102.0                               | 360044                                   | 1                                          | 1656119700                       |
| 20220616230547849                                 | 20220616230547849_0_8                              | 10016                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10016                                  | 2022-06-15 01:47:15                      | u_9                                         | 10.6                                | 360056                                   | 1                                          | 1654739220                       |
| 20220616230553618                                 | 20220616230553618_0_17                             | 10005                                            |                                                    | 4d6e4f9a-43eb-4468-9ace-64fc0e9445dc_0-1-0_20220616230831972.parquet | 10005                                  | 2022-06-15 01:15:15                      | u_1                                         | 13.9                                | 360013                                   | 1                                          | 1656119700                       |
+---------------------------------------------------+----------------------------------------------------+--------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------+------------------------------------------+---------------------------------------------+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------+
12 rows selected (0.072 seconds)
0: jdbc:hive2://hadoop102:10000>



