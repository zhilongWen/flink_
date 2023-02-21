package com.at.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @create 2023-02-21
 */
public class CountWindowsBaseEventTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*
            Count Window 计数窗口

                窗口指定条数触发窗口计算，并清除前滑动步长个元素，滚动窗口步长等于窗口大小



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
                .countWindow(10, 5)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        System.out.println("当前 watermark = " + context.currentWatermark() + " --> " + new Timestamp(context.currentWatermark()) + " ::: current time = " + new Timestamp(System.currentTimeMillis()));

                        Timestamp win = new Timestamp(context.currentWatermark());

                        long cnt = elements.spliterator().getExactSizeIfKnown();

                        out.collect("key = " + key + "\t window [ " + win + " ) 有 " + cnt + " 条元素");
                    }
                });

        env.execute();


    }

}
