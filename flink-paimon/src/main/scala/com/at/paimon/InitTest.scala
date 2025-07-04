package com.at.paimon

import org.apache.flink.configuration.{CheckpointingOptions, Configuration, StateBackendOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object InitTest {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val defaultConfig = new Configuration
    defaultConfig.setString("rest.bind-port", "8081")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig)
    val settings = EnvironmentSettings.newInstance.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(3, // 10 seconds restart window
      10000))

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

    env.addSource(new SourceFunction[String] {

        override def run(ctx: SourceFunction.SourceContext[String]) = {

          while (true) {
            ctx.collect("Hello, world!")
            Thread.sleep(1000)
          }
        }

        override def cancel() = {

        }
      })
      .print()


    env.execute("InitTest")
  }
}
