package com.at.iceberg.demo;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
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

import java.util.concurrent.atomic.AtomicInteger;

public class IcebergWriteLocalFileDemo {
    public static void main(String[] args) throws Exception {

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
        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration checkpointConfig = new Configuration();
//        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://127.0.0.1:8020/tmp/checkpoints");
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/wenzhilong/warehouse/space/flink_/flink-iceberg/checkpoints");
        env.configure(checkpointConfig);


        String warehouse = "file:/Users/wenzhilong/warehouse/space/flink_/flink-iceberg/db";
        CatalogLoader catalogLoader = CatalogLoader.hadoop(
                "iceberg_hadoop",
                new org.apache.hadoop.conf.Configuration(),
                ImmutableMap.of("warehouse", warehouse)

        );

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "t");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        if (catalog.tableExists(tableIdentifier)) {
            catalog.dropTable(tableIdentifier, true);
        }

        Schema schema = new Schema(
                Types.NestedField.optional(0, "id", Types.IntegerType.get()),
                Types.NestedField.optional(1, "name", Types.StringType.get()),
                Types.NestedField.optional(2, "behavior", Types.StringType.get())
        );

        Table table = catalog.createTable(
                tableIdentifier,
                schema,
//                PartitionSpec.builderFor(schema).identity("id").build(),
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
        );

        DataStream<RowData> icebergWriteSource = getIcebergWriteSource(env);

//        icebergWriteSource
//                .print();
//
        FlinkSink.forRowData(icebergWriteSource)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();

        env.execute("Iceberg Write Demo");
    }

    public static SingleOutputStreamOperator<RowData> getIcebergWriteSource(StreamExecutionEnvironment env) {
        return env.addSource(
                        new ParallelSourceFunction<RowData>() {

                            @Override
                            public void run(SourceContext<RowData> sourceContext) throws InterruptedException {

                                AtomicInteger atomicInteger = new AtomicInteger(0);

                                while (true) {

                                    int id = atomicInteger.getAndIncrement();
//                                    if (id == 10) {
//                                        atomicInteger.set(0);
//                                    }

                                    for (int i = 0; i < 10; i++) {
                                        int id_1 = id * 10 + i;
                                        GenericRowData rowData = new GenericRowData(3);
                                        rowData.setField(0, id_1);
                                        rowData.setField(1, StringData.fromString("name_" + id_1));
                                        rowData.setField(2, StringData.fromString("behavior_" + id_1));
                                        sourceContext.collect(rowData);

                                    }


                                    Thread.sleep(1000);
                                }
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                )
                .uid("iceberg_write_source")
                .setParallelism(3);
    }
}
