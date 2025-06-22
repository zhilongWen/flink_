package com.at.writehive;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class IcebreagWriteHiveTableDemo222 {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration defaultConfig = new Configuration();
//        defaultConfig.setString("rest.bind-port", "8083");
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

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("user_behaviors")
                .setGroupId("IcebreagWriteHiveTableDemo")
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.timestamp(1750525590262L))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<UserBehavior> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "user_behaviors")
                .uid("user_behaviors_Kafka_Source")
                .name("user_behaviors_Kafka_Source")
                .setParallelism(4)
                .map(new RichMapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        return JSON.parseObject(value, UserBehavior.class);
                    }
                })
                .uid("user_behaviors_parse")
                .name("user_behaviors_parse")
                .setParallelism(4);

        Schema tableSchema = Schema.newBuilder()
                .column("userId", DataTypes.INT())
                .column("itemId", DataTypes.BIGINT())
                .column("categoryId", DataTypes.INT())
                .column("behavior", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                // 把 ts 转换成 TIMESTAMP(3)，作为事件时间字段
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(ts, 3)")
                // 定义 watermark 策略（这里是延迟 5 秒）
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build();
//        Table table = tableEnv.fromDataStream(sourceStream, tableSchema);
//        tableEnv.createTemporaryView("user_behaviors", table);

//        tableEnv.executeSql("select * from user_behaviors_view").print();

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String catalogName = "hive_catalog";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/wenzhilong/warehouse/space/flink_/conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(catalogName, hive);

        tableEnv.useCatalog(catalogName);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("iceberg_db");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("create table if not exists hive_stream_table_user_behaviors(\n"
                + "        user_id int,\n"
                + "        item_id bigint,\n"
                + "        category_id int,\n"
                + "        behavior string,\n"
                + "        ts bigint\n"
                + ")COMMENT 'flink create table hive_stream_table_user_behaviors'\n"
                + "PARTITIONED BY (`dt` string,`hm` string,`mm` string)\n"
                + "STORED AS PARQUET\n"
                + "LOCATION '/warehouse/iceberg_db.db/hive_stream_table_user_behaviors'\n"
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
                + ")\n");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.createTemporaryView("user_behaviors_view", sourceStream, tableSchema);

        tableEnv.executeSql("insert into hive_stream_table_user_behaviors\n"
                + "select userId as user_id,\n"
                + "       itemId as item_id,\n"
                + "       categoryId as category_id,\n"
                + "       behavior,\n"
                + "       ts,\n"
                + "       DATE_FORMAT(event_time, 'yyyy-MM-dd') as dt,\n"
                + "       DATE_FORMAT(event_time, 'HH') as hm,\n"
                + "       DATE_FORMAT(event_time, 'mm') as mm\n"
                + "from user_behaviors_view");


//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        tableEnv.executeSql("select * from user_behaviors_view").print();


//        env.execute("IcebergWriteHdfsFileDemo");
    }
}
