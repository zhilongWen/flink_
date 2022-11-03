package com.at.conntctors.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-02
 */
public class KafkaConnectionSQLT1 {



    public static void main(String[] args) throws Exception {

        // 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // 设置状态后端
//        env.setStateBackend(new org.apache.flink.runtime.state.filesystem.FsStateBackend("file:///D:\\workspace\\flink_\\ck"));
//        // 每隔 10min 做一次 checkpoint 模式为 AT_LEAST_ONCE
//        env.enableCheckpointing(10 * 60 * 1000L, CheckpointingMode.AT_LEAST_ONCE);
//
//
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        // 设置 checkpoint 最小间隔周期 1min
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        // 设置 checkpoint 必须在 1min 内完成，否则会被丢弃
//        checkpointConfig.setCheckpointTimeout(60 * 1000L);
//        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
//        checkpointConfig.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
//        // 设置 checkpoint 的并发度为 1
//        checkpointConfig.setMaxConcurrentCheckpoints(1);


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
                + "    'topic' = 'test-1',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-1_upsert-kafka_test-group-id',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "     'json.ignore-parse-errors' = 'true'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select * from kafka_source_tbl").print();




        env.execute();


    }

}



