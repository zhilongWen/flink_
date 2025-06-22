package com.at.writehive;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class IcebreagWriteHiveTableDemo {
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
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
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
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://10.211.55.102:8020/tmp/checkpoints");
        env.configure(checkpointConfig);

        String catalogName = "hive_catalog";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/wenzhilong/warehouse/space/flink_/conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(catalogName, hive);

        tableEnv.useCatalog(catalogName);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("iceberg_db");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String sourceSQL = "create table if not exists user_behaviors_view(\n"
                + "   userId int,\n"
                + "   itemId bigint,\n"
                + "   categoryId int,\n"
                + "   behavior string,\n"
                + "   ts bigint,\n"
                + "   row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000),'yyyy-MM-dd HH:mm:ss'),\n"
                + "   watermark for row_time as row_time - interval '60' second\n"
                + ")with(\n"
                + "        'connector' = 'kafka',\n"
                + "        'topic' = 'user_behaviors',\n"
                + "        'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "        'properties.group.id' = 'IcebreagWriteHiveTableDemo',\n"
                + "        'scan.startup.mode' = 'timestamp',\n"
                + "        'scan.startup.timestamp-millis' = '1750525590262',\n"
                + "        'format' = 'json',\n"
                + "        'json.ignore-parse-errors' = 'true'\n"
                + ")";

        String hiveSinkSQL = "create table if not exists hive_stream_table_user_behaviors(\n"
                + "        user_id int,\n"
                + "        item_id bigint,\n"
                + "        category_id int,\n"
                + "        behavior string,\n"
                + "        ts bigint\n"
                + ")COMMENT 'flink create table hive_stream_table_user_behaviors'\n"
                + "PARTITIONED BY (`dt` string,`hm` string,`mm` string)\n"
                + "STORED AS PARQUET\n"
                + "LOCATION '/warehouse/iceberg_db/hive_stream_table_user_behaviors'\n"
                + "TBLPROPERTIES (\n"
                + "        -- using default partition-name order to load the latest partition every 12h (the most recommended and convenient way)\n"
                + "        'streaming-source.enable' = 'true',\n"
                + "        'streaming-source.partition.include' = 'latest',\n"
                + "        'streaming-source.monitor-interval' = '1 min',\n"
                + "        'streaming-source.partition-order' = 'partition-name',  -- option with default value, can be ignored.\n"
                + "\n"
                + "        -- Parquet + Snappy 配置\n"
                + "        'parquet.compression' = 'SNAPPY',\n"
                + "        'partition.time-extractor.timestamp-pattern' = '$dt $hm:$mm:00',\n"
                + "        'sink.partition-commit.trigger'='partition-time',\n"
                + "        'sink.partition-commit.delay'='1 min',\n"
                + "        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n"
                + "        'sink.partition-commit.policy.kind'='metastore,success-file'\n"
                + ")";

        String insertSQL = "insert into hive_stream_table_user_behaviors\n"
                + "select userId as user_id,\n"
                + "       itemId as item_id,\n"
                + "       categoryId as category_id,\n"
                + "       behavior,\n"
                + "       ts,\n"
                + "       DATE_FORMAT(row_time, 'yyyy-MM-dd') as dt,\n"
                + "       DATE_FORMAT(row_time, 'HH') as hm,\n"
                + "       DATE_FORMAT(row_time, 'mm') as mm\n"
                + "from user_behaviors_view";

        tableEnv.executeSql(sourceSQL);
//        tableEnv.executeSql("select * from user_behaviors_view").print();

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(hiveSinkSQL);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(insertSQL);


        env.execute("IcebergWriteHdfsFileDemo");
    }
}
