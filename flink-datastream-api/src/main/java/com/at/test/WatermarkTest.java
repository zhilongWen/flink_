package com.at.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.time.Duration;
import java.util.Properties;

/**
 * @create 2022-05-30
 */
public class WatermarkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test-watermark", new SimpleStringSchema(), properties);

        kafkaConsumer.setStartFromLatest();
        FlinkKafkaConsumerBase<String> kafkaConsumerBase = kafkaConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(" ")[1]) * 1000L;
                            }
                        })
        );

        DataStreamSource<String> streamSource = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = streamSource
                .map(s -> {
                    String[] elems = s.split(" ");
                    return Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));


        streamOperator.print();

        env.execute();

    }

}
