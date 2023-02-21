package com.at.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @create 2023-02-21
 */
public class SlidingWindowsBaseEventTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
            Sliding Windows 滚动窗口

                1.窗口重叠
                2.每隔滑动步长秒触发窗口计算，但会保留窗口部分元素
                3.下边界 移动到 上边界 是清空下边界的所有元素

         */


        SingleOutputStreamOperator<Tuple2<String, Long>> withWatermarkStream = env
                .socketTextStream("127.0.0.1", 9099)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String val) throws Exception {
                        String[] elems = val.split(" ");
                        return Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> elem, long l) {
                                        return elem.f1;
                                    }
                                })
                );

        SingleOutputStreamOperator<String> countWinProcessStream = withWatermarkStream
                .keyBy(r -> r.f0)
                // 窗口大小为 5 s，窗口滑动步长为 3 s
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(3)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                        System.out.println("当前 watermark = " + context.currentWatermark() + " --> " + new Timestamp(context.currentWatermark()) + " ::: current time = " + new Timestamp(System.currentTimeMillis()));

                        Timestamp winStart = new Timestamp(context.window().getStart());
                        Timestamp winEnd = new Timestamp(context.window().getEnd());

                        long cnt = elements.spliterator().getExactSizeIfKnown();

                        out.collect("key = " + key + "\t window [ " + winStart + " - " + winEnd + " ) 有 " + cnt + " 条元素");


                    }
                });

        countWinProcessStream.print();


        env.execute();


    }

}
