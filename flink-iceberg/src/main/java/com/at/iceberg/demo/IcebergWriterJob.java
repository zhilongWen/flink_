package com.at.iceberg.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types.NestedField;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergWriterJob {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");


        // 1. 创建执行环境
        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig);
        env.enableCheckpointing(10000); // 10秒一次checkpoint
        env.setParallelism(2);

        // 2. 创建Kafka源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("merged_table_source_topic_1")
                .setGroupId("WriteIcebergTable_1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. 读取数据流并解析
        SingleOutputStreamOperator<Tuple4<Long, String, Integer, Long>> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "merged_table_source_topic_1")
                .map(new MapFunction<String, Tuple4<Long, String, Integer, Long>>() {
                    @Override
                    public Tuple4<Long, String, Integer, Long> map(String value) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            return Tuple4.of(
                                    jsonObject.getLong("id"),
                                    jsonObject.getString("name"),
                                    jsonObject.getInteger("col_a"),
                                    jsonObject.getLong("ts")
                            );
                        } catch (Exception e) {
                            // 记录解析错误
                            System.err.println("Failed to parse record: " + value);
                            return null;
                        }
                    }
                })
                .filter(value -> value != null) // 过滤掉解析失败的记录
                .name("ParseKafkaData")
                .uid("ParseKafkaData")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Long, String, Integer, Long>>(Time.seconds(5)) {
                            @Override
                            public long extractTimestamp(Tuple4<Long, String, Integer, Long> element) {
                                return element.f3; // 使用ts字段作为事件时间
                            }
                        }
                );

        // 4. 定义Iceberg表结构
        Schema icebergSchema = new Schema(
                NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
                NestedField.optional(2, "name", org.apache.iceberg.types.Types.StringType.get()),
                NestedField.optional(3, "col_a", org.apache.iceberg.types.Types.IntegerType.get())
        );

        // 5. 配置Iceberg Catalog
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

        // 6. 创建或加载Iceberg表
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "merged_table_source_1");

        Table table;
        if (!catalog.tableExists(tableIdentifier)) {
            // 创建表属性
            Map<String, String> properties = new HashMap<>(3);
            properties.put(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
            // 启用upsert
            properties.put(TableProperties.UPSERT_ENABLED, "true");
            properties.put(TableProperties.FORMAT_VERSION, "2");

            // 创建表
            table = catalog.createTable(
                    tableIdentifier,
                    icebergSchema,
                    PartitionSpec.unpartitioned(),
                    properties
            );
            System.out.println("Created new table: " + tableIdentifier);
        } else {
            table = catalog.loadTable(tableIdentifier);
            System.out.println("Loaded existing table: " + tableIdentifier);
        }

        // 7. 创建TableLoader
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        // 8. 转换为RowData流
        RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);
        DataStream<RowData> rowDataStream = sourceStream
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
                .uid("ConvertToRowData");

        // 9. 添加日志输出
        rowDataStream
                .process(new ProcessFunction<RowData, Void>() {
                    @Override
                    public void processElement(RowData value, Context ctx, Collector<Void> out) {
                        System.out.println("Processing record: " + value);
                    }
                })
                .name("LogRecords")
                .uid("LogRecords");

        // 10. 配置并创建Iceberg Sink
        FlinkSink.forRowData(rowDataStream)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .upsert(true)
                .equalityFieldColumns(Collections.singletonList("id"));
//                .overwrite(false)
//                .append()
        // 主键字段
//                .equalityFieldColumns(Arrays.asList("id"))
//                .writeParallelism(1)
//                .build();

        // 11. 执行任务
        env.execute("Iceberg Writer Job (Flink 1.16, Iceberg 1.5.0)");
    }
}
