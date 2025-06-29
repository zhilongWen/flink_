package com.at.iceberg.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wenzhilong
 */
public class WriteIcebergTable_1 {
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
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://10.211.55.102:8020/tmp/checkpoints/WriteIcebergTable_1");
        env.configure(checkpointConfig);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("merged_table_source_topic_1")
                .setGroupId("WriteIcebergTable_1")
                .setStartingOffsets(OffsetsInitializer.latest())
//                .setStartingOffsets(OffsetsInitializer.timestamp(1750952622345L))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<RowData> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "merged_table_source_topic_1")
                .uid("merged_table_source_topic_1")
                .name("merged_table_source_topic_1")
                .setParallelism(2)
                // {"id":1,"name":"tom","col_a":10,"ts":1751203952588}
                .map(new MapFunction<String, Tuple4<Long, String, Integer, Long>>() {
                    @Override
                    public Tuple4<Long, String, Integer, Long> map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return Tuple4.of(jsonObject.getLongValue("id"), jsonObject.getString("name"), jsonObject.getInteger("col_a"), jsonObject.getLongValue("ts"));
                    }
                })
                .uid("parse_merged_table_source_topic_1")
                .name("parse_merged_table_source_topic_1")
                .setParallelism(2)
                // watermark: ts
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<Long, String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long recordTimestamp) {
                                        return element.f3;
                                    }
                                })
                )
                //  转换为RowData流
                .map(new MapFunction<Tuple4<Long, String, Integer, Long>, RowData>() {
                    @Override
                    public RowData map(Tuple4<Long, String, Integer, Long> value) throws Exception {
                        GenericRowData rowData = new GenericRowData(3);
                        rowData.setField(0, value.f0); // id
                        rowData.setField(1, StringData.fromString(value.f1)); // name
                        rowData.setField(2, value.f2); // col_a
                        return rowData;
                    }
                })
                .returns(TypeInformation.of(RowData.class))
                .name("ConvertToRowData")
                .uid("ConvertToRowData")
                .setParallelism(2);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "col_a", Types.IntegerType.get())
        );

        // hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop/default/merged_table_source
        String warehouse = "hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop";
        // 创建Hadoop配置
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://10.211.55.102:8020");
        hadoopConf.set("dfs.client.use.datanode.hostname", "true");
        hadoopConf.set("dfs.datanode.use.datanode.hostname", "true");
        CatalogLoader catalogLoader = CatalogLoader.hadoop(
                "iceberg_hadoop",
                hadoopConf,
                ImmutableMap.of("warehouse", warehouse)
        );

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "merged_table_source_1");
//
//        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
//        Table table = catalog.createTable(
//                tableIdentifier,
//                schema,
////                PartitionSpec.builderFor(schema).build(),
//                PartitionSpec.unpartitioned(),
//                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
//        );
//

        Table table;
        if (catalog.tableExists(tableIdentifier)) {
            table = catalog.loadTable(tableIdentifier);
            System.out.println("Loaded existing table: " + tableIdentifier);
        } else {
            // 创建表属性
            Map<String, String> properties = new HashMap<>(3);
            properties.put(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
            // 启用upsert
            properties.put(TableProperties.UPSERT_ENABLED, "true");
            properties.put(TableProperties.FORMAT_VERSION, "2");

            table = catalog.createTable(
                    tableIdentifier,
                    schema,
                    PartitionSpec.unpartitioned(),
                    properties
            );
            System.out.println("Created new table: " + tableIdentifier);
        }
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        FlinkSink.forRowData(sourceStream)
                .table(table)
                .tableLoader(tableLoader)
                .upsert(true)
                .writeParallelism(1)
                .upsert(true)
                .equalityFieldColumns(Collections.singletonList("id"));
//                .append();

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
         * CREATE EXTERNAL TABLE user_behavior_flink_1 (user_id int,item_id bigint,behavior string,ts bigint)
         * STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
         * LOCATION 'hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop/default/user_behavior_flink_1'
         * TBLPROPERTIES ('iceberg.catalog'='location_based_table');
         */

        env.execute("WriteIcebergTable");
    }
}
