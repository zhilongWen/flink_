package com.at.iceberg.demo;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

public class IcebergWriteHdfsFileDemo {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig);

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
                .setGroupId("IcebergWriteHdfsFileDemo")
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.timestamp(1750525590262L))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<UserBehavior> sourceStream = env
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

        String warehouse = "hdfs://10.211.55.102:8020/tmp/iceberg_warehouse";
        CatalogLoader catalogLoader = CatalogLoader.hadoop(
                "iceberg_warehouse",
                new org.apache.hadoop.conf.Configuration(),
                ImmutableMap.of("warehouse", warehouse)
        );

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "user_behaviors");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        if (catalog.tableExists(tableIdentifier)) {
            catalog.dropTable(tableIdentifier, true);
        }

        Schema schema = new Schema(
                Types.NestedField.optional(0, "user_id", Types.IntegerType.get()),
                Types.NestedField.optional(1, "item_id", Types.LongType.get()),
                Types.NestedField.optional(2, "category_id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "behavior", Types.StringType.get()),
                Types.NestedField.optional(4, "ts", Types.LongType.get())
        );

        Table table = catalog.createTable(
                tableIdentifier,
                schema,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
        );

        FlinkSink.builderFor(
                        sourceStream,
                        new MapFunction<UserBehavior, RowData>() {
                            @Override
                            public RowData map(UserBehavior value) throws Exception {
                                GenericRowData genericRowData = new GenericRowData(5);
                                genericRowData.setField(0, value.getUserId());
                                genericRowData.setField(1, value.getItemId());
                                genericRowData.setField(2, value.getCategoryId());
                                genericRowData.setField(3, StringData.fromString(value.getBehavior()));
                                genericRowData.setField(4, value.getTs());
                                return genericRowData;
                            }
                        },
                        TypeInformation.of(RowData.class)
                )
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(2)
                .append();

        env.execute("Iceberg Write HDFS File Demo");
    }
}
