package com.at.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * @create 2022-05-15
 */
public class SourceFromSocket {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .socketTextStream("127.0.0.1",8099);

        env.execute();

    }

}
