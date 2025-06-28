package com.at.iceberg;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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

import java.time.Duration;

/**
 * @author wenzhilong
 */
public class WriteIcebergTable2 {
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
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://10.211.55.102:8020/tmp/checkpoints/WriteIcebergTable2");
        env.configure(checkpointConfig);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("user_behaviors")
                .setGroupId("WriteIcebergTable2")
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.timestamp(1750952622345L))
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
                .setParallelism(4)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        Schema schema = new Schema(
                Types.NestedField.optional(0, "user_id", Types.IntegerType.get()),
                Types.NestedField.optional(1, "item_id", Types.LongType.get()),
                Types.NestedField.optional(2, "category_id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "behavior", Types.StringType.get()),
                Types.NestedField.optional(4, "ts", Types.LongType.get())
        );

        // hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop/default/user_behavior_flink_2
        String warehouse = "hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop";
        CatalogLoader catalogLoader = CatalogLoader.hadoop(
                "iceberg_hadoop",
                new org.apache.hadoop.conf.Configuration(),
                ImmutableMap.of("warehouse", warehouse)
        );

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "user_behavior_flink_2");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
        Table table = catalog.createTable(
                tableIdentifier,
                schema,
//                PartitionSpec.builderFor(schema).build(),
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
                .set("write.target-file-size-bytes", "128MB")  // 增大目标文件大小
                .set("write.distribution-mode", "hash")        // 数据按分区哈希分布
                .set("write.metadata.delete-after-commit.enabled", "true")  // 自动删除旧元数据
                .writeParallelism(2)
                .append();

        /**
         * hive load
         *
         * set iceberg.catalog.iceberg_hive.type=hive;
         * set iceberg.catalog.iceberg_hive.uri=thrift://10.211.55.102:9083;
         * set iceberg.catalog.iceberg_hive.clients=10;
         * set iceberg.catalog.iceberg_hive.warehouse=hdfs://hadoop102:8020/user/hive/warehouse/iceberg_hive;
         *
         * set iceberg.catalog.iceberg_hadoop.type=hadoop;
         * set iceberg.catalog.iceberg_hadoop.warehouse=hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop;
         *
         * 指定路径加载 iceberg table
         * CREATE EXTERNAL TABLE user_behavior_flink_2 (user_id int,item_id bigint,behavior string,ts bigint)
         * STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
         * LOCATION 'hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop/default/user_behavior_flink_2'
         * TBLPROPERTIES ('iceberg.catalog'='location_based_table');
         */

        env.execute("WriteIcebergTable2");
    }
}
