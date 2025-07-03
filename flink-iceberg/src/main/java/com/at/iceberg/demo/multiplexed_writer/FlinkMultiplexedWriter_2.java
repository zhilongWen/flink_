package com.at.iceberg.demo.multiplexed_writer;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class FlinkMultiplexedWriter_2 {

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

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Configuration checkpointConfig = new Configuration();
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-iceberg/checkpoints");
        env.configure(checkpointConfig);


        // {"user_id":1,"action":"search","timestamp":1751555900000,"details":"1-1"} 2025-07-03 23:18:20
        // {"user_id":2,"action":"click","timestamp":1751555900000,"details":"1-2"}  2025-07-03 23:18:20
        // {"user_id":3,"action":"search","timestamp":1751555900000,"details":"1-3"} 2025-07-03 23:18:20
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("multiplexed_writer_2")
                .setGroupId("FlinkMultiplexedWriter_2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // org.apache.flink.table.data.RowData
        SingleOutputStreamOperator<RowData> sourceDataStream = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source"
                )
                .name("kafka-source")
                .uid("kafka-source")
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, RowData>() {
                    @Override
                    public void flatMap(String value, Collector<RowData> out) throws Exception {
                        try {
                            UserAction action = JSON.parseObject(value, UserAction.class);
                            GenericRowData row = new GenericRowData(4);
                            row.setField(0, action.user_id);
                            row.setField(1, StringData.fromString(action.action));
                            row.setField(2, TimestampData.fromEpochMillis(action.timestamp));
                            row.setField(3, StringData.fromString(action.details));
                            out.collect(row);
                        } catch (Exception e) {
                            System.out.println("source data " + value + " is not valid");
                        }
                    }
                })
                .setParallelism(2)
                .name("parse source data")
                .uid("parse-source-data");

//        sourceDataStream.print();

        org.apache.hadoop.conf.Configuration icebergConf = new org.apache.hadoop.conf.Configuration();
        icebergConf.set("commit.chain.multiplex-enabled", "true");
        icebergConf.set("commit.chain.group-size", "5");
        icebergConf.set("commit.group-id", "flink_realtime_group");

        // 加载Iceberg表
        TableLoader tableLoader = TableLoader.fromHadoopTable(
                "hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop/test_db/user_actions",
                icebergConf
        );

        // 创建Iceberg Sink
        FlinkSink.forRowData(sourceDataStream)
                .tableLoader(tableLoader)
                .overwrite(false)
                .append();

        // 执行任务
        env.execute("FlinkMultiplexedWriter_2");
    }
}
