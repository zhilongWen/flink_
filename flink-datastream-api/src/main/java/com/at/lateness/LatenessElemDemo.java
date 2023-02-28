package com.at.lateness;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class LatenessElemDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> withWatermarkStream = env
                .socketTextStream("127.0.0.1", 9008)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] elems = value.split(" ");
                        collector.collect(Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000L));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> value, long l) {
                                                return value.f1;
                                            }
                                        }
                                )
                );

        SingleOutputStreamOperator<String> countWinStream = withWatermarkStream
                .keyBy(
                        new KeySelector<Tuple2<String, Long>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Long> value) throws Exception {
                                return value.f0;
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .allowedLateness(Time.seconds(1L))
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {

                            private ValueState<Boolean> firstArriveTriggerWin;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                firstArriveTriggerWin = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("firstArriveTriggerWin", TypeInformation.of(Boolean.class)));
                            }

                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());


                                if (firstArriveTriggerWin.value() == null) {
                                    out.collect("key = " + key + "， win [ " + winStart + " - " + winEnd + " ) 第一次触发，窗口中有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素");
                                    firstArriveTriggerWin.update(true);
                                } else {
                                    out.collect("key = " + key + "， win [ " + winStart + " - " + winEnd + " ) 迟到数据触发，窗口中有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素");
                                }

                            }

                            @Override
                            public void clear(Context context) throws Exception {
                                firstArriveTriggerWin.clear();
                            }
                        });

        // watermark = 当前窗口观察到的最大时间 - 最大延迟时间 - 1L

        countWinStream.print();

        env.execute();

        /*

nc -lk 9008
w 1
w 5
w 6
w 2 > 迟到数据触发
w 4 > 迟到数据触发
w 7
w 1 > 迟到数据丢弃
w 2 > 迟到数据丢弃
w 10
w 11
w 9
w 12
key = w， win [ 1970-01-01 08:00:00.0 - 1970-01-01 08:00:05.0 ) 迟到数据触发，窗口中有 3 条元素
key = w， win [ 1970-01-01 08:00:05.0 - 1970-01-01 08:00:10.0 ) 第一次触发，窗口中有 3 条元素
key = w， win [ 1970-01-01 08:00:05.0 - 1970-01-01 08:00:10.0 ) 迟到数据触发，窗口中有 4 条元素
         */

    }

}
