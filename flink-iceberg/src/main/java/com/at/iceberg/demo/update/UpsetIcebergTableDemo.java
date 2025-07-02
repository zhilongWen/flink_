package com.at.iceberg.demo.update;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UpsetIcebergTableDemo {
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
        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration checkpointConfig = new Configuration();
//        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://127.0.0.1:8020/tmp/checkpoints");
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-iceberg/checkpoints");
        env.configure(checkpointConfig);


        tableEnv.executeSql("create catalog hadoop_catalog with (\n"
                + "  'type'='iceberg',\n"
                + "  'catalog-type'='hadoop',\n"
                + "  'warehouse'='hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop',\n"
                + "  'property-version'='1'\n"
                + ")");

        tableEnv.executeSql("use catalog hadoop_catalog");

        tableEnv.executeSql("show tables").print();

//        tableEnv.executeSql("CREATE TABLE if not exists sample_3 (\n"
//                + "  id INT UNIQUE COMMENT 'unique id',\n"
//                + "  data STRING ,\n"
//                + "  id_type STRING ,\n"
//                + "  PRIMARY KEY(id) NOT ENFORCED\n"
//                + ") with (\n"
//                + "  'format-version'='2', \n"
//                + "  'write.upsert.enabled'='true'\n"
//                + ")");
//
//        tableEnv.executeSql("INSERT INTO sample_3 VALUES (1, 'a',cast(null as VARCHAR)), (2, 'b',cast(null as VARCHAR)), (3, 'c',cast(null as VARCHAR)), (4, 'd',cast(null as    VARCHAR)), (5, 'e',cast(null as VARCHAR))");

//        tableEnv.executeSql("SELECT * FROM sample_3").print();

//        tableEnv.executeSql("INSERT INTO sample_3 VALUES (1, 'c',cast(null as VARCHAR))");

//        tableEnv.executeSql("SELECT * FROM sample_3").print();

//        tableEnv.executeSql("INSERT INTO sample_3 VALUES (2, 'c','2-2')");

//        tableEnv.executeSql("SELECT * FROM sample_3").print();
        //+----+-------------+--------------------------------+--------------------------------+
        //| op |          id |                           data |                        id_type |
        //+----+-------------+--------------------------------+--------------------------------+
        //| +I |           2 |                              c |                            2-2 |
        //| +I |           1 |                              c |                         <NULL> |
        //| +I |           3 |                              c |                         <NULL> |
        //| +I |           4 |                              d |                         <NULL> |
        //| +I |           5 |                              e |                         <NULL> |
        //+----+-------------+--------------------------------+--------------------------------+

        tableEnv.executeSql("create temporary view tmp_view as select 1 as id,'1-1' as id_type");
//        tableEnv.executeSql(" select * from tmp_view").print();
        tableEnv.executeSql("MERGE INTO sample_3 \n"
                + "USING (SELECT id, id_type FROM tmp_view) AS a \n"
                + "ON sample_3.id = a.id \n"
                + "WHEN MATCHED THEN UPDATE SET id_type = a.id_type  \n"
                + "WHEN NOT MATCHED THEN INSERT (id, id_type) VALUES (a.id, a.id_type)");

        tableEnv.executeSql("SELECT * FROM sample_3").print();

//        Exception in thread "main" org.apache.flink.table.api.ValidationException: SQL validation failed. validateImpl() returned null
//        at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.org$apache$flink$table$planner$calcite$FlinkPlannerImpl$$validate(FlinkPlannerImpl.scala:186)
//        at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.validate(FlinkPlannerImpl.scala:113)
//        at org.apache.flink.table.planner.operations.SqlToOperationConverter.convert(SqlToOperationConverter.java:261)
//        at org.apache.flink.table.planner.delegation.ParserImpl.parse(ParserImpl.java:106)
//        at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:723)
//        at com.at.iceberg.demo.update.UpsetIcebergTableDemo.main(UpsetIcebergTableDemo.java:105)
//        Caused by: java.lang.IllegalArgumentException: validateImpl() returned null
//        at org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkArgument(Preconditions.java:142)
//        at org.apache.calcite.sql.validate.AbstractNamespace.validate(AbstractNamespace.java:85)
//        at org.apache.calcite.sql.validate.SqlValidatorImpl.validateNamespace(SqlValidatorImpl.java:997)
//        at org.apache.calcite.sql.validate.AbstractNamespace.getRowType(AbstractNamespace.java:115)


    }
}
