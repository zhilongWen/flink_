package com.at.processfunction;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

/**
 * @create 2023-02-26
 */
public class WinElemsCountProcessWindowFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> withWatermarkStream = env
                .fromCollection(
                        Arrays.asList(
                                Tuple2.of("Alick", 1),
                                Tuple2.of("Alick", 2),
                                Tuple2.of("Alick", 3),
                                Tuple2.of("BOb", 3),
                                Tuple2.of("BOb", 5),
                                Tuple2.of("BOb", 7),
                                Tuple2.of("BOb", 10),
                                Tuple2.of("Alick", 7)
                        )
                )
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple2<String, Integer>>() {
                            @Override
                            public WatermarkGenerator<Tuple2<String, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Integer>>() {

                                    private Long delay = 0L;
                                    private Long watermark = -Long.MAX_VALUE + delay + 1L;

                                    @Override
                                    public void onEvent(Tuple2<String, Integer> event, long eventTimestamp, WatermarkOutput output) {
                                        watermark = Math.max(event.f1,watermark);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        output.emitWatermark(new Watermark(watermark - delay - 1L));
                                    }
                                };
                            }

                            @Override
                            public TimestampAssigner<Tuple2<String, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new TimestampAssigner<Tuple2<String, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                                        return element.f1 * 1000L;
                                    }
                                };
                            }
                        }
                );
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
//                                    @Override
//                                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
//                                        return element.f1 * 1000L;
//                                    }
//                                })
//                );


        KeyedStream<Tuple2<String, Integer>, String> keyedStream = withWatermarkStream
                .keyBy(r -> r.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> winStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<String> countWinElemsStream = winStream
                .process(
                        new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());

                                long count = elements.spliterator().getExactSizeIfKnown();

                                out.collect("key = " + key + " , win [ " + winStart + " - " + winEnd + " ) 有 " + count + " 个元素");

                            }
                        }
                );

        countWinElemsStream.print();


        env.execute();


    }

}
