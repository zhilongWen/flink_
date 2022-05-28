package com.at.processfunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @create 2022-05-17
 */
public class ProcessJoinFunctions {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 点击流
        KeyedStream<Tuple3<String, String, Long>, String> clickStream = env
                .fromElements(
                        Tuple3.of("Bob", "click", 3600 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                )
                .keyBy(click -> click.f0);

        KeyedStream<Tuple3<String, String, Long>, String> browseStream = env
                .fromElements(
                        Tuple3.of("Bob", "browse", 2000 * 1000L),
                        Tuple3.of("Bob", "browse", 3000 * 1000L),
                        Tuple3.of("Bob", "browse", 3500 * 1000L),
                        Tuple3.of("Bob", "browse", 4000 * 1000L),
                        Tuple3.of("Bob", "browse", 4200 * 1000L),
                        Tuple3.of("Bob", "browse", 7000 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                )
                .keyBy(browse -> browse.f0);


        clickStream
                .intervalJoin(browseStream)
                // browse.ts - 600 < clickStream.ts < browse.ts + 500
                //          3000   <     3600       <   4100
                .between(Time.seconds(-600), Time.seconds(500))
                .process(
                        new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                            @Override
                            public void processElement(Tuple3<String, String, Long> click, Tuple3<String, String, Long> browse, Context ctx, Collector<String> out) throws Exception {

                                out.collect(click + " -> " + browse);

                            }
                        }
                )
                .print();


        env.execute();

    }

}
