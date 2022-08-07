package com.at;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-08-07
 */
public class FlinkHiveConnectorWriteTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

//        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck"));
//        env.getCheckpointConfig().setCheckpointInterval(2 * 60 * 1000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().enableUnalignedCheckpoints(true);

        // 设置状态后端
//        env.setStateBackend(new org.apache.flink.runtime.state.filesystem.FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck"));
//        // 每隔 10min 做一次 checkpoint 模式为 AT_LEAST_ONCE
//        env.enableCheckpointing(1 * 60 * 1000L, CheckpointingMode.AT_LEAST_ONCE);
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


        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("testdb");


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


        String sourceSQL = "create table if not exists  kafka_source_tbl(\n"
                + "    id bigint,\n"
                + "    name string,\n"
                + "    address string,\n"
                + "    ts bigint,\n"
                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000),'yyyy-MM-dd HH:mm:ss'),\n"
                + "    watermark for row_time as row_time - interval '10' second \n"
                + ")with(\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'hive-logs',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'hive-logs-group-id',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select\n"
                + "    id,\n"
                + "    name,\n"
                + "    address,\n"
                + "    ts,\n"
                + "    date_format(row_time,'yyyyMMdd'),\n"
                + "    date_format(row_time,'HH')\n"
                + "from kafka_source_tbl").print();



    }

}
