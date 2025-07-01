package com.at.iceberg.demo;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UpsetIcebergTableDemo_2 {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setParallelism(2);

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
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3 * 1000);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(3 * 1000);
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
        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration checkpointConfig = new Configuration();
//        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://127.0.0.1:8020/tmp/checkpoints");
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-iceberg/checkpoints");
        env.configure(checkpointConfig);


        // ./kafka-console-producer.sh --bootstrap-server hadoop102:9092 --topic upset_table_topic
        // {"word":"a","cnt":1}
        //  {"word":"b","cnt":1}
        //  {"word":"c","cnt":1}
        //  {"word":"d","cnt":1}
        //  {"word":"e","cnt":1}
        //
        //
        //  {"word":"a","cnt":10}
        //  {"word":"e","cnt":11}
        String sourceSQL = "CREATE TABLE if not exists default_catalog.default_database.kafka_source_tbl(\n"
                + "    word STRING,\n"
                + "    cnt INT\n"
                + ") \n"
                + "WITH \n"
                + "(\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'upset_table_topic',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'UpsetIcebergTableDemo_2',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

        String catalog = "create catalog hadoop_catalog with (\n"
                + "  'type'='iceberg',\n"
                + "  'catalog-type'='hadoop',\n"
                + "  'warehouse'='hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop',\n"
                + "  'property-version'='1'\n"
                + ")";
        tableEnv.executeSql(catalog);

        tableEnv.executeSql("use catalog hadoop_catalog");
        String ddl = "CREATE TABLE if not exists word_count_tbl (\n"
                + "  word STRING UNIQUE COMMENT 'unique id',\n"
                + "  cnt INT NOT NULL,\n"
                + "  PRIMARY KEY(word) NOT ENFORCED\n"
                + ") with (\n"
                + "  'format-version'='2', \n"
                + "  'write.upsert.enabled'='true'\n"
                + ")";
        tableEnv.executeSql(ddl);

        tableEnv.executeSql("use catalog default_catalog");
        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("use catalog hadoop_catalog");
        String sinkSQL = "insert into word_count_tbl select word,cnt from default_catalog.default_database.kafka_source_tbl";
        tableEnv.executeSql(sinkSQL);


        env.execute();
    }
}
