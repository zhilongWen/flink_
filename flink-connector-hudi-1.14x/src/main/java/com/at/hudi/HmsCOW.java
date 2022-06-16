package com.at.hudi;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-06-15
 */
public class HmsCOW {



    public static void main(String[] args) {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive",hive);


        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String sourceSQL ="create table if not exists hudi_user_behavior_tbl\n"
                + "(\n"
                + "    userId     int,\n"
                + "    itemId     bigint,\n"
                + "    categoryId int,\n"
                + "    behavior string,\n"
                + "    ts         bigint,\n"
                + "    row_time   as to_timestamp(from_unixtime(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n"
                + "    watermark for row_time as row_time - interval '1' second\n"
                + ")\n"
                + "with (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'user_behaviors',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-group-id',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";


        String hudiSinkSQL = "create table if not exists user_behavior_hms_cow\n"
                + "(\n"
                + "    user_id     int,\n"
                + "    item_id     bigint,\n"
                + "    category_id int,\n"
                + "    behavior string,\n"
                + "    ts          bigint,\n"
                + "    `dt` string,\n"
                + "    `hh` string,\n"
                + "    `mm` string\n"
                + ")partitioned by (`dt`,`hh`,`mm`)\n"
                + "with(\n"
                + "    'connector'='hudi',\n"
                + "    'path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_hms_cow_tbl',\n"
                + "    'table.type'='COPY_ON_WRITE',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出\n"
                + "    'hoodie.datasource.write.recordkey.field' = 'user_id',\n"
                + "    'write.precombine.field'= 'ts',\n"
                + "    'write.tasks'='1',\n"
                + "    'write.rate.limit'= '100',\n"
                + "    'compaction.tasks'='1',\n"
                + "    'compaction.async.enabled'= 'true',\n"
                + "    'compaction.trigger.strategy'= 'num_and_time',\n"
                + "    'compaction.delta_commits'= '1',\n"
                + "    'read.streaming.enabled' = 'true',\n"
                + "    'read.streaming.check-interval' = '4',\n"
                + "    'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc\n"
                + "    'hive_sync.enable'='true',           -- required，开启hive同步功能\n"
                + "    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,\n"
                + "    'hive_sync.table'='user_behavior_hms_cow_tbl',                          -- required, hive 新建的表名\n"
                + "    'hive_sync.db'='default',                       -- required, hive 新建的数据库名\n"
                + "    'hive_sync.support_timestamp'= 'true'\n"
                + ")\n";


        String insertSQL = "insert into user_behavior_hms_cow\n"
                + "select\n"
                + "    userId as user_id,\n"
                + "    itemId as item_id,\n"
                + "    categoryId as category_id,\n"
                + "    behavior,\n"
                + "    ts,\n"
                + "    date_format(cast(row_time as string),'yyyyMMdd') dt,\n"
                + "    date_format(cast(row_time as string),'HH') hh,\n"
                + "    date_format(cast(row_time as string),'mm') mm\n"
                + "from hudi_user_behavior_tbl";

        tableEnv.executeSql(sourceSQL);
        tableEnv.executeSql(hudiSinkSQL);
        tableEnv.executeSql(insertSQL);




    }


}
