package com.at.testp;

import com.alibaba.fastjson2.JSONObject;
import com.at.WriteHiveTestBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @create 2022-08-07
 */
public class KafkaConsumerTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics(Arrays.asList("hive-logs"))
                .setGroupId("hive-logs-group-id")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka source")
                .map(r -> JSONObject.parseObject(r, WriteHiveTestBean.class))
                .returns(WriteHiveTestBean.class)
                .print();


        env.execute();


    }


}
