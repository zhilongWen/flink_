package com.at.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.*;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @create 2022-05-15
 */
public class SourceFromKafka {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        // old
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "test-group-id");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test-topic", new SimpleStringSchema(), properties);
//        env.addSource(kafkaConsumer).print();


        FlinkKafkaConsumerBase<String> stringFlinkKafkaConsumerBase = new FlinkKafkaConsumerBase<String>(
                Arrays.asList("test-group"),
                null,
                new KafkaDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                1L,
                true
        ) {
            @Override
            protected AbstractPartitionDiscoverer createPartitionDiscoverer(KafkaTopicsDescriptor topicsDescriptor, int indexOfThisSubtask, int numParallelSubtasks) {
                return null;
            }

            @Override
            protected boolean getIsAutoCommitEnabled() {
                return false;
            }

            @Override
            protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(Collection collection, long timestamp) {
                return null;
            }

            @Override
            protected AbstractFetcher createFetcher(SourceContext sourceContext, Map subscribedPartitionsToStartOffsets, SerializedValue watermarkStrategy, StreamingRuntimeContext runtimeContext, OffsetCommitMode offsetCommitMode, MetricGroup kafkaMetricGroup, boolean useMetrics) throws Exception {
                return null;
            }
        };




        // new
//        KafkaSource<String> source = KafkaSource
//                .<String>builder()
//                .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
//                .setGroupId("MyGroup")
//                .setTopics(Arrays.asList(TOPIC1, TOPIC2))
//                .setDeserializer(new TestingKafkaRecordDeserializationSchema())
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .build();
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics(Arrays.asList("test-topic"))
                .setGroupId("test-group-id")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();



        DataStreamSource<String> streamSource = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        streamSource.print();


        env.execute();

    }

}
