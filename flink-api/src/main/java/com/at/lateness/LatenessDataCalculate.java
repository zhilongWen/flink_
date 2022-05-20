package com.at.lateness;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @create 2022-05-20
 */
public class LatenessDataCalculate {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        /*

            q 1
            q 2
            q 5
            q 8
            q 10 -> watermark = 10s - 5s - 1ms = 4999 触发 [0,5) 窗口计算 并不关闭窗口
            q 1 ->  [0,5) late 触发窗口计算
            q 3 ->  [0,5) late 触发窗口计算
            q - 15 -> 15s - 5s - 5s - 1ms = 4999 关闭 [0,5) 窗口
            q 1 -> [0,5) late 不触发窗口 将数据发送到侧输出流

         */

        SingleOutputStreamOperator<String> result = env
                .socketTextStream("127.0.0.1", 9099)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] elems = value.split(" ");
                        return Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5l))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                                return element.f1;
                                            }
                                        }
                                )
                )
                .keyBy(e -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5L))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late") {
                })
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                                // 初始化一个窗口状态变量，注意：窗口状态变量的可见范围是当前窗口
                                ValueState<Boolean> firstCalculate = context.windowState().getState(new ValueStateDescriptor<Boolean>("first-calculate", Types.BOOLEAN));


                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());

                                long count = elements.spliterator().getExactSizeIfKnown();

                                if (firstCalculate.value() == null) {

                                    out.collect("window [ " + winStart + " - " + winEnd + " ) 第一次触发计算 watermark = " + context.currentWatermark() + " 窗口中有 " + count + " 条数据");
                                    firstCalculate.update(true);
                                } else {
                                    out.collect("window [ " + winStart + " - " + winEnd + " ) 迟到数据触发计算 watermark = " + context.currentWatermark() + " 窗口中有 " + count + " 条数据");
                                }

                            }
                        }
                );

        result.print();

        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late") {
        }).print("late：");


        env.execute();


    }

}
