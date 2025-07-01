package com.at.iceberg.demo;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UpsertIcebergTableStreamDemo {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8081");
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
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3 * 1000);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(3 * 1000);
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

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("upset_table_topic")
                .setGroupId("UpsertIcebergTableStreamDemo")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<RowData> sourceDataStream = env.fromSource(
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
                            WordCount wordCount = JSON.parseObject(value, WordCount.class);
                            GenericRowData rowData = new GenericRowData(2);
                            rowData.setField(0, StringData.fromString(wordCount.getWord()));
                            rowData.setField(1, wordCount.getCnt());

//                            out.collect(Row.of(wordCount.getWord(), wordCount.getCnt()));
                            out.collect(rowData);
                        } catch (Exception e) {
                            System.out.println("source data " + value + " is not valid");
                        }
                    }
                })
                .setParallelism(2)
                .name("parse source data")
                .uid("parse-source-data");

        //org.apache.iceberg.hadoop.HadoopCatalog
        String warehouse = "hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://10.211.55.102:8020");
        hadoopConf.set("dfs.client.use.datanode.hostname", "true");
        hadoopConf.set("dfs.datanode.use.datanode.hostname", "true");
        CatalogLoader catalogLoader = CatalogLoader.hadoop(
                "iceberg_hadoop",
                hadoopConf,
                ImmutableMap.of("warehouse", warehouse)
        );

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "word", Types.StringType.get()),
                Types.NestedField.required(2, "cnt", Types.IntegerType.get())
        );

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "word_count_tbl_2");
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
            properties.put("primary-key", "word");

            table = catalog.createTable(
                    tableIdentifier,
                    schema,
                    PartitionSpec.unpartitioned(),
                    properties
            );
            System.out.println("Created new table: " + tableIdentifier);
        }
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        FlinkSink.forRowData(sourceDataStream)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .upsert(true)
                .equalityFieldColumns(Collections.singletonList("word"))
                .append();

        env.execute("UpsertIcebergTableStreamDemo");
    }
}
