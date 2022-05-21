package com.at.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @create 2022-05-21
 */
public class JoinIntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> clickStream = env
                .fromElements(
                        Tuple3.of("user-1", "click", 12 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0L)).withTimestampAssigner((Tuple3<String, String, Long> element, long recordTimestamp) -> element.f2)
                );


        SingleOutputStreamOperator<Tuple3<String, String, Long>> browseStream = env
                .fromElements(
                        Tuple3.of("user-1", "browse", 1 * 60 * 1000L),
                        Tuple3.of("user-1", "browse", 7 * 60 * 1000L),
                        Tuple3.of("user-1", "browse", 10 * 60 * 1000L),
                        Tuple3.of("user-1", "browse", 11 * 60 * 1000L),
                        Tuple3.of("user-1", "browse", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0L)).withTimestampAssigner((Tuple3<String, String, Long> element, long recordTimestamp) -> element.f2)
                );



        clickStream
                .keyBy(r -> r.f0)
                .intervalJoin(browseStream.keyBy(r -> r.f0))

                // browse + 10  < clickTs  < browse + 15
                //          2   <   12     < 17
                .between(Time.minutes(-10),Time.minutes(5))
                .process(
                        new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                            @Override
                            public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                                out.collect(left + " -> " + right);
                            }
                        }
                )
                .print();


        env.execute();


    }

}
