package com.at.lateness;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @create 2022-05-20
 */
public class LatenessDataOutput {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<String> result = env
                .socketTextStream("127.0.0.1", 9099)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] elems = value.split(" ");
                        return Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000L);
                    }
                })

                // watermark = 观察到的最大时间戳 - 最大延时时间 - 1ms

                // flink 认为时间戳小于水位线的事件都已到达

                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                /*
                                    q 1
                                    q 2
                                    q 5  -> watermark = 5s - 0s - 1ms = 4999ms  flink 认为时间戳小于水位线的事件都已到达  关闭 [0,5) 的窗口
                                    q 1  -> late data
                                 */

//                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))

                                // 5s 迟到数据

                                /*
                                    q 1
                                    q 2
                                    q 5 -> watermark = 5s - 5s - 1ms
                                    q 1
                                    q 9 -> watermark = 9s - 5s - 1ms = 3999ms
                                    q 10 -> watermark = 10s - 5s - 1ms = 4999ms 关闭 [0,5) 的窗口
                                    q 1 -> late data
                                */

                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))

                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                                return element.f1;
                                            }
                                        }
                                )
                )
                .keyBy(elem -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late") {
                })
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                                out.collect("当前 watermark = " + context.currentWatermark() + " 窗口中有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据");

                            }
                        }
                );

        result.print();

        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late") {
        }).print("late：");


        env.execute();


    }

}
