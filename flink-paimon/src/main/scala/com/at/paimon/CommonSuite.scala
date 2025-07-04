package com.at.paimon

import org.apache.flink.configuration.{CheckpointingOptions, Configuration, StateBackendOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

trait CommonSuite {

  def initStreamEnv(): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val defaultConfig = new Configuration
    defaultConfig.setString("rest.bind-port", "8081")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig)
    val settings = EnvironmentSettings.newInstance.inStreamingMode.build
//    val settings = EnvironmentSettings.newInstance.inBatchMode().build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 设置 Hive 元数据相关参数// 设置 Hive 元数据相关参数
    tableEnv.getConfig.getConfiguration.setString("hive.metastore.disallow.incompatible.col.type.changes", "false")
    tableEnv.getConfig.getConfiguration.setString("hive.strict.managed.tables", "false")
    tableEnv.getConfig.getConfiguration.setString("metastore.create.as.acid", "false")


    env.setRestartStrategy(
      org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
        3,
        // 10 seconds restart window
        10000
      )
    )

    // start a checkpoint every 1000 ms// start a checkpoint every 1000 ms
    env.enableCheckpointing(1000)
    // advanced options:// advanced options:
    // set mode to exactly-once (this is the default)// set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // make sure 500 ms of progress happen between checkpoints// make sure 500 ms of progress happen between checkpoints
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10 * 1000)
    // checkpoints have to complete within one minute, or are discarded// checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(10 * 1000)
    // only two consecutive checkpoint failures are tolerated// only two consecutive checkpoint failures are tolerated
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    // allow only one checkpoint to be in progress at the same time// allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // enable externalized checkpoints which are retained// enable externalized checkpoints which are retained
    // after job cancellation// after job cancellation
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // enables the unaligned checkpoints// enables the unaligned checkpoints
    //        env.getCheckpointConfig().enableUnalignedCheckpoints();//        env.getCheckpointConfig().enableUnalignedCheckpoints();
    // sets the checkpoint storage where checkpoint snapshots will be written// sets the checkpoint storage where checkpoint snapshots will be written
    val checkpointConfig = new Configuration
    //        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");//        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    //        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");//        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    //        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://127.0.0.1:8020/tmp/checkpoints");//        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://127.0.0.1:8020/tmp/checkpoints");
    checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap")
    checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem")
    checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-paimon/checkpoints")
    env.configure(checkpointConfig)

    tableEnv.executeSql(
      """
        |create catalog fs_paimon_catalog with (
        |  'type'='paimon',
        |  'warehouse'='hdfs://10.211.55.102:8020/user/hive/warehouse/paimon_db.db/fs'
        |)
        |""".stripMargin)

    tableEnv.executeSql(
      """
        |create catalog hive_paimon_catalog with (
        |    'type' = 'paimon',
        |    'metastore' = 'hive',
        |    'uri'=  'thrift://hadoop102:9083',
        |    'hive-conf-dir'='/Users/wenzhilong/warehouse/space/flink_/conf',
        |    'warehouse' = 'hdfs://10.211.55.102:8020/user/hive/warehouse'
        |)
        |""".stripMargin)

    tableEnv.executeSql("""use catalog hive_paimon_catalog""")



    (env, tableEnv)
  }
}
