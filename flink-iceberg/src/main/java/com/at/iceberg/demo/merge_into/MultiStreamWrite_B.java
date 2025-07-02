package com.at.iceberg.demo.merge_into;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MultiStreamWrite_B {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8082");
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
        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration checkpointConfig = new Configuration();
//        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://127.0.0.1:8020/tmp/checkpoints");
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-iceberg/checkpoints");
        env.configure(checkpointConfig);

        // {"word":"c","word_type":"3-1"}
        // {"word":"d","word_type":"3-1"}
        // {"word":"g","word_type":"3-1"}
        String sourceSQL = "CREATE TABLE if not exists default_catalog.default_database.source_b(\n"
                + "    word STRING,\n"
                + "    word_type STRING\n"
                + ") \n"
                + "WITH \n"
                + "(\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'upset_table_topic_b',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'MultiStreamWrite_B',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);
//        tableEnv.executeSql("select * from default_catalog.default_database.source_b").print();

        String catalog = "create catalog hadoop_catalog with (\n"
                + "  'type'='iceberg',\n"
                + "  'catalog-type'='hadoop',\n"
                + "  'warehouse'='hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop',\n"
                + "  'property-version'='1'\n"
                + ")";
        tableEnv.executeSql(catalog);

        tableEnv.executeSql("USE CATALOG hadoop_catalog");
        tableEnv.executeSql("USE test_db");
//        tableEnv.executeSql("INSERT INTO word_stats " +
//                "SELECT word, CAST(NULL AS BIGINT), word_type " +
//                "FROM default_catalog.default_database.source_b");


//        String mergeSql = "MERGE INTO hadoop_catalog.test_db.word_stats target " +
//                "USING default_catalog.default_database.source_b source " +
//                "ON target.word = source.word " +
//                "WHEN MATCHED THEN UPDATE SET target.word_type = source.word_type " +
//                "WHEN NOT MATCHED THEN INSERT (word, cnt, word_type) VALUES (source.word, NULL, source.word_type)";
        // 修正后的MERGE语句（显式指定完整路径）
//        String mergeSql = "MERGE INTO hadoop_catalog.test_db.word_stats target " +
//                "USING (SELECT word, word_type FROM default_catalog.default_database.source_b  ) source " +
//                "ON target.word = source.word " +
//                "WHEN MATCHED THEN UPDATE SET target.word_type = source.word_type " +
//                "WHEN NOT MATCHED THEN INSERT (word, cnt, word_type) " +
//                "VALUES (source.word, (SELECT cnt FROM hadoop_catalog.test_db.word_stats WHERE word = source.word), source.word_type)";
//        tableEnv.executeSql(mergeSql);





//        String insertSql = "INSERT INTO word_stats (word, cnt, word_type) " +
//                "SELECT word, CAST(NULL AS BIGINT), word_type FROM source_b " +
//                "ON DUPLICATE KEY UPDATE word_type = VALUES(word_type)";
//        tableEnv.executeSql(insertSql);
        // 方法1：使用INSERT...SELECT实现UPSERT
//        String upsertSql = "INSERT INTO word_stats (word, cnt, word_type) " +
//                "SELECT " +
//                "  b.word, " +
//                "  COALESCE(a.cnt, 0), " +  // 保持cnt不变，若不存在则设为0
//                "  b.word_type " +
//                "FROM source_b b " +
//                "LEFT JOIN word_stats a ON a.word = b.word " +
//                "ON DUPLICATE KEY UPDATE word_type = VALUES(word_type)";
//        tableEnv.executeSql(upsertSql);

//    // 方法2：分两步实现（先查询再插入）
        tableEnv.executeSql("CREATE TEMPORARY VIEW tmp_upsert AS " +
                "SELECT " +
                "  b.word, " +
                "  a.cnt, " +
                "  b.word_type " +
                "FROM default_catalog.default_database.source_b b " +
                "LEFT JOIN word_stats a ON a.word = b.word");

        tableEnv.executeSql("INSERT INTO word_stats " +
                "SELECT word, cnt, word_type FROM tmp_upsert " +
                "ON DUPLICATE KEY UPDATE word_type = VALUES(word_type)");

    }
}
