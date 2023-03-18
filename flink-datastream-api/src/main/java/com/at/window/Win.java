package com.at.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author zero
 * @create 2023-03-02
 */
public class Win {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env
                .socketTextStream("hadoop102", 9099);

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapStream = streamSource
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                                String[] elems = value.split(" ");
                                out.collect(Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000));
                            }
                        }
                );

        SingleOutputStreamOperator<Tuple2<String, Long>> withWatermarkStream = flatMapStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                                return element.f1;
                                            }
                                        }
                                )
                );

        KeyedStream<Tuple2<String, Long>, String> keyedStream = withWatermarkStream
                .keyBy(
                        new KeySelector<Tuple2<String, Long>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Long> value) throws Exception {
                                return value.f0;
                            }
                        }
                );

//        withWatermarkStream
//                .windowAll(org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create());
//
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> winStream = keyedStream
////                .countWindow(5, 2)
////                .countWindow(5)
////                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(5L)));
////                .window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
////                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
////                .window(SlidingProcessingTimeWindows.of(Time.seconds(5L),Time.seconds(2L)))
                .window(SlidingEventTimeWindows.of(Time.seconds(5L), Time.seconds(2L)));
//
        SingleOutputStreamOperator<String> countWinStream = winStream
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());

                                long count = elements.spliterator().getExactSizeIfKnown();

                                out.collect("key = " + key + ", win [ " + winStart + " - " + winEnd + " ), 窗口有 " + count + " 条元素，当前的 watermark = " + context.currentWatermark());

                            }
                        }
                );

        countWinStream
                .print();


//        WindowedStream<Tuple2<String, Long>, String, GlobalWindow> winStream = keyedStream
//                .countWindow(5, 2);
//
//        winStream
//                .process(
//                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
//                            @Override
//                            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
//                                out.collect("key = " + key + ", 窗口有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素 ");
//                            }
//                        }
//                )
//                .print();


        env.execute();


    }

}
