package com.at.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.omg.CORBA.LongHolder;

/**
 * @create 2022-05-21
 */
public class JoinWindowJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> streamOne = env
                .fromElements(
                        Tuple2.of("a", 1L),
                        Tuple2.of("b", 1L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((Tuple2<String, Long> elem, long ts) -> elem.f1 * 1000L)
                );


        SingleOutputStreamOperator<Tuple2<String, Long>> streamTwo = env
                .fromElements(
                        Tuple2.of("a", 1L),
                        Tuple2.of("a", 2L),
                        Tuple2.of("a", 5L),
                        Tuple2.of("a", 7L),
                        Tuple2.of("b", 4L),
                        Tuple2.of("b", 6L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((Tuple2<String, Long> elem, long ts) -> elem.f1 * 1000L)
                );


        streamOne
                .join(streamTwo)
                .where(r -> r.f0)
                .equalTo(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> first, Tuple2<String, Long> second) throws Exception {
                        return first + " -> " + second;
                    }
                })
                .print();


        env.execute();


    }

}
