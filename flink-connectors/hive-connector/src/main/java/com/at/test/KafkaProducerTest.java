package com.at.test;

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

    private static Long count = 0L;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

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
//                                                    random.nextLong() & Long.MAX_VALUE ,
                                                    random.nextInt() & Integer.MAX_VALUE,
                                                    System.currentTimeMillis() /*+ 3 * 3600 * 1000*/ ,
                                                    UUID.randomUUID().toString().substring(1, 5),
                                                    UUID.randomUUID().toString().substring(2, 7).toUpperCase(Locale.ROOT))

                                    );

                                    if(((++count) % 1000 )== 0){
                                        System.out.println("count：" + (++count) + " " + System.currentTimeMillis());
                                    }

                                    try { TimeUnit.MILLISECONDS.sleep(300); } catch (InterruptedException e) { e.printStackTrace(); }

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
//                .setBootstrapServers("hdfs01:9092,hdfs02:9092,hdfs03:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
//                                .setTopic("flink-write-hive-test-topic")
//                                .setTopic("hive-logs")
                                .setTopic("flink-retime-write-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        sourceStream
//                .print();
                .sinkTo(kafkaSink);

        System.out.println(System.currentTimeMillis());


        env.execute();


    }

}
