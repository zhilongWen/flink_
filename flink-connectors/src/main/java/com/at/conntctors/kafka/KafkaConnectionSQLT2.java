package com.at.conntctors.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-02
 */
public class KafkaConnectionSQLT2 {



    public static void main(String[] args) throws Exception {

        // 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //table 环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String sourceSQL = "CREATE TABLE kafka_source_tbl(\n"
                + "    `userId` INT,\n"
                + "    `itemId` BIGINT,\n"
                + "    `behavior` STRING,\n"
                + "    `ts` BIGINT,\n"
                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '1' SECOND\n"
                + ") WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'test-2',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-2_0_upsert-kafka_test-group-id',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "     'json.ignore-parse-errors' = 'true'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select * from kafka_source_tbl").print();




        env.execute();


    }

}



