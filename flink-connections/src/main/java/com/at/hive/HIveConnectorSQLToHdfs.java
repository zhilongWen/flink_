package com.at.hive;

import com.at.util.EnvironmentUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-07
 */
public class HIveConnectorSQLToHdfs {

    public static void main(String[] args) {


        //--execute.mode stream --enable.checkpoint true --enable.table.env true --checkpoint.type fs --checkpoint.interval 6000 -Xmx 100m -Xms 100m --checkpoint.dir hdfs://hadoop102:8020/user/flink/checkpoint/

        EnvironmentUtil.Environment environment = EnvironmentUtil.getExecutionEnvironment(args);

        StreamExecutionEnvironment env = environment.getEnv();
        StreamTableEnvironment tableEnv = environment.getTableEnv();

//        String sourceSQL = "CREATE TABLE IF not exists kafka_source_tbl(\n"
//                + "    `userId` INT,\n"
//                + "    `itemId` BIGINT,\n"
//                + "    `categoryId` INT,\n"
//                + "    `behavior` STRING,\n"
//                + "    `ts` BIGINT,\n"
//                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),\n"
//                + "    WATERMARK FOR row_time AS row_time - INTERVAL '1' SECOND\n"
//                + ") WITH (\n"
//                + "    'connector' = 'kafka',\n"
//                + "    'topic' = 'user_behaviors',\n"
//                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
//                + "    'properties.group.id' = 'test-group-id',\n"
//                + "    'scan.startup.mode' = 'earliest-offset',\n"
////                + "    'scan.startup.mode' = 'timestamp',\n"
////                + "    'scan.startup.timestamp-millis' = '1654089523897',\n"
//                + "    'format' = 'json',\n"
//                + "     'json.ignore-parse-errors' = 'true'\n"
//                + ")";

        String sourceSQL = "CREATE TABLE  IF not exists  file_source_tbl(\n"
                + "    user_id BIGINT,\n"
                + "    item_id BIGINT,\n"
                + "    category_id BIGINT,\n"
                + "    behavior STRING,\n"
                + "    ts BIGINT,\n"
                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss')),\n"
                + "    watermark for row_time as row_time - INTERVAL '1' SECOND \n"
                + ") WITH (\n"
                + "    'connector' = 'filesystem',   \n"
                + "    'path' = 'file:///D:\\workspace\\flink_\\files\\UserBehavior.csv',\n"
                + "    'format' = 'csv',\n"
                + "    'csv.ignore-parse-errors' = 'true',\n"
                + "    'csv.allow-comments' = 'true'\n"
                + ")";

        String sinkSQL = "create table if not exists user_behavior_tbl\n"
                + "(\n"
                + "    user_id     bigint,\n"
                + "    item_id     int,\n"
                + "    category_id bigint,\n"
                + "    behavior string,\n"
                + "    ts          bigint,\n"
                + "    `dt` string,\n"
                + "    `hm` string\n"
                + ") partitioned by (`dt`,`hm`)\n"
                + "with(\n"
                + "    'connector'='filesystem',\n"
                + "    'path'='hdfs://hadoop102:8020/user/hive/warehouse/user_behavior_tbl/',\n"
                + "    'format'='orc',\n"
                + "    'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',\n"
                + "    'sink.partition-commit.delay'='1 min',\n"
                + "    'sink.partition-commit.trigger'='partition-time',\n"
                + "    'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- Assume user configured time zone is 'Asia/Shanghai'\n"
                + "    'sink.partition-commit.policy.kind'='success-file'\n"
                + ")\n";


        String insertSQL = "insert into user_behavior_tbl\n"
                + "select CAST(user_id AS BIGINT)     as user_id,\n"
                + "       CAST(item_id AS INT)        as item_id,\n"
                + "       CAST(category_id AS BIGINT) as category_id,\n"
                + "       behavior,\n"
                + "       ts,\n"
                + "       DATE_FORMAT(row_time, 'yyyyMMdd'),\n"
                + "       DATE_FORMAT(row_time, 'HH')\n"
                + "from file_source_tbl";


        tableEnv.executeSql(sourceSQL);
        tableEnv.executeSql(sinkSQL);
        tableEnv.executeSql(insertSQL);

//        tableEnv.executeSql("select * from file_source_tbl").print();

        /*

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


set hive.msck.path.validation=ignore;
msck repair table user_behavior_tbl;
show partitions user_behavior_tbl;

         */


    }

}
