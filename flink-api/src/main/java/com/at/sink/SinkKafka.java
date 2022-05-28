package com.at.sink;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;

import java.util.concurrent.ExecutionException;

/**
 * @create 2022-05-27
 */
public class SinkKafka {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        new FlinkKafkaProducer<>()

        KafkaSink.builder();


        env.execute();


    }

}
