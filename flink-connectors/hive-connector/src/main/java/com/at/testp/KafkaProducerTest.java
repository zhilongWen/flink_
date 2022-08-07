package com.at.testp;

import com.alibaba.fastjson2.JSON;
import com.at.WriteHiveTestBean;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @create 2022-08-07
 */
public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<String> sourceStream = env
                .addSource(
                        new SourceFunction<WriteHiveTestBean>() {

                            private boolean isRunning = true;
                            private Random random = new Random();

                            @Override
                            public void run(SourceContext<WriteHiveTestBean> ctx) throws Exception {

                                while (isRunning) {

                                    ctx.collect(
                                            WriteHiveTestBean.of(
                                                    random.nextLong() & Long.MAX_VALUE ,
                                                    System.currentTimeMillis(),
                                                    UUID.randomUUID().toString().substring(1, 5),
                                                    UUID.randomUUID().toString().substring(2, 7).toUpperCase(Locale.ROOT))
                                    );

                                    try { TimeUnit.SECONDS.sleep(20); } catch (InterruptedException e) { e.printStackTrace(); }

                                }

                            }

                            @Override
                            public void cancel() {
                                isRunning = false;
                            }
                        }
                )
                .map(r -> JSON.toJSONString(r))
                .returns(String.class);

        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("hive-test-logs")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        sourceStream
//                .print();
                .sinkTo(kafkaSink);


        env.execute();


    }

}
