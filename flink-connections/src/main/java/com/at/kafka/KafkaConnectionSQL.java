package com.at.kafka;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-02
 */
public class KafkaConnectionSQL {

    /*
        json
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-json</artifactId>
			<version>${flink.version}</version>
			<scope>provided</scope>
		</dependency>

        sql
 		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-planner_${scala.binary.version}</artifactId>
			<version>${flink.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-api-java</artifactId>
			<version>${flink.version}</version>
		</dependency>



     */


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
                + "    `categoryId` INT,\n"
                + "    `behavior` STRING,\n"
                + "    `ts` BIGINT,\n"
                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '1' SECOND\n"
                + ") WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'user_behaviors',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-group-id',\n"
                + "    'scan.startup.mode' = 'timestamp',\n"
                + "    'scan.startup.timestamp-millis' = '1654089523897',\n"
                + "    'format' = 'json',\n"
                + "     'json.ignore-parse-errors' = 'true'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);

//        tableEnv.executeSql("SELECT TO_TIMESTAMP(FROM_UNIXTIME(1513135677789 / 1000, 'yyyy-MM-dd HH:mm:ss'))").print();


//        tableEnv.executeSql("SELECT\n"
//                + "    *\n"
//                + "FROM \n"
//                + "TABLE(\n"
//                + "    HOP(\n"
//                + "        TABLE kafka_source_tbl,\n"
//                + "        DESCRIPTOR(row_time),\n"
//                + "        INTERVAL '20' SECOND,\n"
//                + "        INTERVAL '60' SECOND\n"
//                + "    )\n"
//                + ")");

//        tableEnv.executeSql("SELECT\n"
//                + "    *\n"
//                + "FROM\n"
//                + "TABLE(\n"
//                + "    HOP(\n"
//                + "        DATA => TABLE kafka_source_tbl,\n"
//                + "        TIMECOL => DESCRIPTOR(row_time),\n"
//                + "        SLIDE => INTERVAL '20' SECOND,\n"
//                + "        SIZE => INTERVAL '60' SECOND\n"
//                + "    )\n"
//                + ")")
//                .print();

        System.out.println("=========================>");

        tableEnv.executeSql("SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    count(distinct userId) as uv\n"
                + "FROM \n"
                + "TABLE(\n"
                + "    HOP(\n"
                + "        DATA => TABLE kafka_source_tbl,\n"
                + "        TIMECOL => DESCRIPTOR(row_time),\n"
                + "        SLIDE => INTERVAL '20' SECOND,\n"
                + "        SIZE => INTERVAL '60' SECOND\n"
                + "    )\n"
                + ")\n"
                + "GROUP BY userId,window_start,window_end")
                .print();


        env.execute();


    }

}
