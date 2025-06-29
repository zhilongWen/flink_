package com.at.iceberg.demo;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IcebergWriteHiveTableDemo_2 {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                        3,
                        // 10 seconds restart window
                        10000
                )
        );

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000);
        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained
        // after job cancellation
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // enables the unaligned checkpoints
//        env.getCheckpointConfig().enableUnalignedCheckpoints();
        Configuration checkpointConfig = new Configuration();
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-iceberg/checkpoints");
        env.configure(checkpointConfig);

        // 创建 Hive Catalog
        tableEnv.executeSql(" CREATE CATALOG hive_catalog WITH (\n"
                + "                        'type' = 'iceberg',\n"
                + "                        'catalog-impl' = 'org.apache.iceberg.hive.HiveCatalog',\n"
                + "                        'uri' = 'thrift://hadoop102:9083',\n"
                + "                        'warehouse' = 'hdfs://hdfs://10.211.55.102:8020/user/hive/warehouse'\n"
                + "                    )");

        // 使用 Hive Catalog
        tableEnv.useCatalog("hive_catalog");
        tableEnv.useDatabase("iceberg_db");

        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS iceberg_write_hive_table_demo_2 (\n" +
                        "  id BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                        "  name STRING,\n" +
                        "  colA INT,\n" +
                        "  colB INT,\n" +
                        "  colC STRING,\n" +
                        "  ts BIGINT,\n" +
                        "  dt STRING,\n" +
                        "  hm STRING,\n" +
                        "  mm STRING\n" +
                        ") PARTITIONED BY (dt, hm, mm) \n"
        );

        // 或 tableEnv.useCatalog("default")，视你配置而定
        tableEnv.useCatalog("default_catalog");

        // 注册 Kafka 源表
        tableEnv.executeSql(
                "CREATE TABLE source_table (\n" +
                        "    id BIGINT,\n" +
                        "    name STRING,\n" +
                        "    colA INT,\n" +
                        "    colB INT,\n" +
                        "    colC STRING,\n" +
                        "    ts BIGINT,\n" +
                        "    event_time AS TO_TIMESTAMP_LTZ(ts, 3)\n" +
                        ") WITH (\n" +
                        "    'connector' = 'kafka',\n" +
                        "    'topic' = 'merged_table_source_topic_1',\n" +
                        "    'properties.group.id' = 'IcebergWriteHiveTableDemo_2',\n" +
                        "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
//                        "    'scan.startup.mode' = 'earliest-offset',\n" +
                        "    'scan.startup.mode' = 'latest-offset',\n" +
                        "    'format' = 'json'\n" +
                        ")"
        );

//        tableEnv.executeSql( "SELECT \n" +
//                "    id, \n" +
//                "    name, \n" +
//                "    colA, \n" +
//                "    colB, \n" +
//                "    colC, \n" +
//                "    ts,\n" +
//                "    CAST(DATE_FORMAT(event_time, 'yyyy-MM-dd') AS STRING) AS dt,\n" +
//                "    CAST(DATE_FORMAT(event_time, 'HH') AS STRING) AS hm,\n" +
//                "    CAST(DATE_FORMAT(event_time, 'mm') AS STRING) AS mm\n" +
//                "FROM source_table").print();

        // 4. 切回 HiveCatalog，执行写入 Iceberg 表
        tableEnv.useCatalog("hive_catalog");
        tableEnv.useDatabase("iceberg_db");

        // 写入 Iceberg Hive 表
        tableEnv.executeSql(
                " INSERT INTO iceberg_write_hive_table_demo_2\n" +
                        "SELECT \n" +
                        "    id, \n" +
                        "    name, \n" +
                        "    colA, \n" +
                        "    colB, \n" +
                        "    colC, \n" +
                        "    ts,\n" +
                        "    CAST(DATE_FORMAT(event_time, 'yyyy-MM-dd') AS STRING) AS dt,\n" +
                        "    CAST(DATE_FORMAT(event_time, 'HH') AS STRING) AS hm,\n" +
                        "    CAST(DATE_FORMAT(event_time, 'mm') AS STRING) AS mm\n" +
                        "FROM default_catalog.default_database.source_table"
        );

    }
}
