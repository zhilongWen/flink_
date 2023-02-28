package com.at.lateness;

import org.apache.flink.api.common.eventtime.*;
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
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

public class LatenessElemOutputOtherStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> withWatermarkStream = env
                .socketTextStream("127.0.0.1", 9008)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] elems = value.split(" ");
                        long time = Long.parseLong(elems[1]) * 1000L;


                        Tuple2<String, Long> tuple2 = Tuple2.of(elems[0], time);

                        collector.collect(tuple2);
                    }
                })
                .rebalance()
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {

                                    private long delay = 1000L;

                                    private long maxWatermark = -Long.MAX_VALUE + delay + 1L;

                                    @Override
                                    public void onEvent(Tuple2<String, Long> value, long l, WatermarkOutput watermarkOutput) {
                                        maxWatermark = Math.max(maxWatermark, value.f1);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        long watermark = maxWatermark - delay - 1;
                                        //System.out.println("当前 watermark = " + watermark);
                                        output.emitWatermark(new Watermark(watermark));
                                    }
                                };
                            }

                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new TimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> val, long l) {
                                        return val.f1;
                                    }
                                };
                            }
                        }
                );

        SingleOutputStreamOperator<String> countWinStream = withWatermarkStream
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("later-data-tag") {
                })
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());

                                out.collect("key = " + key + ", win [ " + winStart + " - " + winEnd + " ) ,有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素，当前 watermark = " + context.currentWatermark());

                            }
                        }
                );


        // watermark = 当前窗口观察到的最大时间 - 最大延迟时间 - 1L


        countWinStream.print(">>> ");
        countWinStream.getSideOutput(new OutputTag<Tuple2<String, Long>>("later-data-tag") {
        }).print("later: ");


        env.execute();

/*

        nc -lk 9008
        q 1
        q 2
        q 5
        q 6
            触发窗口 watermark = 6 - 1 - 1ms >>> > key = q, win [ 1970-01-01 08:00:00.0 - 1970-01-01 08:00:05.0 ) ,有 2 条元素，当前 watermark = 4999
        a 1
            later: > (a,1000) ???? 这里为什么会这样，不是 keyBy 了数据吗，a 和 q 应该是不属于同一个窗口
        a 4
            later: > (a,4000)
        a 6
        a 11
            同时触发了 key = q 和 a 的 窗口 ？？？？？
            >>> > key = q, win [ 1970-01-01 08:00:05.0 - 1970-01-01 08:00:10.0 ) ,有 2 条元素，当前 watermark = 9999
            >>> > key = a, win [ 1970-01-01 08:00:05.0 - 1970-01-01 08:00:10.0 ) ,有 1 条元素，当前 watermark = 9999
        q 9
            later: > (a,9000)
        q 16
            >>> > key = a, win [ 1970-01-01 08:00:10.0 - 1970-01-01 08:00:15.0 ) ,有 1 条元素，当前 watermark = 14999
        a 12
            later: > (a,12000)


       为啥 a 和 q 的窗口相互影响了？



        >>> > key = q, win [ 1970-01-01 08:00:00.0 - 1970-01-01 08:00:05.0 ) ,有 2 条元素，当前 watermark = 4999
        later: > (a,1000)
        later: > (a,4000)
        >>> > key = q, win [ 1970-01-01 08:00:05.0 - 1970-01-01 08:00:10.0 ) ,有 2 条元素，当前 watermark = 9999
        >>> > key = a, win [ 1970-01-01 08:00:05.0 - 1970-01-01 08:00:10.0 ) ,有 1 条元素，当前 watermark = 9999
        later: > (q,9000)
        >>> > key = a, win [ 1970-01-01 08:00:10.0 - 1970-01-01 08:00:15.0 ) ,有 1 条元素，当前 watermark = 14999
        later: > (a,12000)

 */
    }

}
