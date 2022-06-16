package com.at.hudi;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-06-16
 */
public class CDCHudi {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);


        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String cdcSourceSQL = "create table if not exists source_order_tbl\n"
                + "(\n"
                + "    db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "    table_name STRING METADATA  FROM 'table_name' VIRTUAL,\n"
                + "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "    order_id      int,\n"
                + "    order_date    TIMESTAMP(0),\n"
                + "    customer_name string,\n"
                + "    price         decimal(10, 5),\n"
                + "    product_id    int,\n"
                + "    order_status  boolean,\n"
                + "    ts TIMESTAMP(3),\n"
                + "    primary key (order_id) NOT ENFORCED,\n"
                + "    watermark for ts as ts - interval '1' second\n"
                + ") WITH (\n"
                + "    'connector' = 'mysql-cdc',\n"
                + "    'hostname' = 'hadoop102',\n"
                + "    'port' = '3306',\n"
                + "    'username' = 'root',\n"
                + "    'password' = 'root',\n"
                + "    'connect.timeout' = '30s',\n"
                + "    'connect.max-retries' = '3',\n"
                + "    'connection.pool.size' = '5',\n"
                + "    'jdbc.properties.useSSL' = 'false',\n"
                + "    'jdbc.properties.characterEncoding' = 'utf-8',\n"
                + "    'database-name' = 'gmall_report',\n"
                + "    'table-name' = 'orders_tbl',\n"
                + "    'server-time-zone'='Asia/Shanghai',\n"
                + "    'debezium.snapshot.mode'='initial'\n"
                + ")";

//        tableEnv.executeSql(cdcSourceSQL);
//        tableEnv.executeSql("select\n"
//                + "    order_id,\n"
//                + "    date_format(CAST(order_date AS STRING),'yyyy-MM-dd HH:mm:dd') order_date,\n"
//                + "    customer_name,\n"
//                + "    price,\n"
//                + "    product_id,\n"
//                + "    if(true,1,0) order_status,\n"
//                + "    UNIX_TIMESTAMP(cast(ts as string),'yyyy-MM-dd HH:mm:dd') ts\n"
//                + "from source_order_tbl").print();


        String hudiSinkSQL = "create table if not exists order_cdc_hms_mor(\n"
                + "    order_id      int,\n"
                + "    order_date    string,\n"
                + "    customer_name string,\n"
                + "    price         double,\n"
                + "    product_id    int,\n"
                + "    order_status  int,\n"
                + "    ts             bigint,\n"
                + "    PRIMARY KEY (`order_id`) NOT ENFORCED\n"
                + ")with(\n"
                + "    'connector'='hudi',\n"
                + "    'path'='hdfs://hadoop102:8020/user/warehouse/order_cdc_hms_mor_tbl', -- path为落地到hdfs的目录路径\n"
                + "    -- COPY_ON_WRITE：数据保存在列式文件中，如parquet。更新时可以定义数据版本或直接重写对应的parquet文件。支持快照读取和增量读取\n"
                + "    -- MERGE_ON_READ：数据保存在列式文件（如parquet) + 行记录级文件（如avro）中。数据更新时，会先将数据写到增量文件，然后会定时同步或异步合并成新的列式文件。支持快照读取和增量读取与读查询优化\n"
                + "    'table.type'='MERGE_ON_READ',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出\n"
                + "    'hoodie.datasource.write.recordkey.field' = 'order_id',  -- 主键 hoodie.datasource.write.recordkey.field为表去重主键，hudi根据这个配置创建数据索引，实现数据去重和增删改。主键相同时，选取write.precombine.field中对应字段的最大值的记录\n"
                + "    'write.precombine.field'= 'ts', -- 合并字段\n"
                + "    -- 写\n"
                + "    'write.tasks'='1',\n"
                + "    'write.bucket_assign.tasks'='1',\n"
                + "    'write.task.max.size'='1024',\n"
                + "    'write.rate.limit'='10000',\n"
                + "    -- 压缩\n"
                + "    'compaction.tasks'='1',\n"
                + "    'compaction.async.enabled'='true',\n"
                + "    'compaction.delta_commits'='1',\n"
                + "    'compaction.max_memory'='500',\n"
                + "    -- stream mode\n"
                + "    'read.streaming.enabled'='true',\n"
                + "    'read.streaming.check.interval'='3',\n"
                + "    -- hive\n"
                + "    'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc\n"
                + "    'hive_sync.enable'='true',           -- required，开启hive同步功能\n"
                + "    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,\n"
                + "    'hive_sync.table'='order_cdc_hms_mor_tbl',                          -- required, hive 新建的表名\n"
                + "    'hive_sync.db'='default',                       -- required, hive 新建的数据库名\n"
                + "    'hive_sync.support_timestamp'= 'true'\n"
                + ")";

        String insertSQL = "insert into order_cdc_hms_mor\n"
                + "select\n"
                + "    order_id,\n"
                + "    date_format(CAST(order_date AS STRING),'yyyy-MM-dd HH:mm:dd') order_date,\n"
                + "    customer_name,\n"
                + "    price,\n"
                + "    product_id,\n"
                + "    if(true,1,0) order_status,\n"
                + "    UNIX_TIMESTAMP(cast(ts as string),'yyyy-MM-dd HH:mm:dd') ts\n"
                + "from source_order_tbl\n";

        tableEnv.executeSql(cdcSourceSQL);
        tableEnv.executeSql(hudiSinkSQL);
        tableEnv.executeSql(insertSQL);

        tableEnv.executeSql("select * from order_cdc_hms_mor").print();





    }

}
