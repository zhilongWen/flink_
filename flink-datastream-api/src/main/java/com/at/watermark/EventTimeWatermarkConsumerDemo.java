package com.at.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
import java.util.Iterator;

/**
 * @create 2023-02-19
 */
public class EventTimeWatermarkConsumerDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // Flink 1.12 默认使用的是 事件时间，具体的操作通过水位线抽取定义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 通过 socket 获取数据源
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9099);

        // 将事件转为 Tuple2 ，f0：words，f1：时间戳 ms
        SingleOutputStreamOperator<Tuple2<String, Long>> mapStream = socketTextStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] elems = value.split(" ");
                return Tuple2.of(elems[0], Long.parseLong(elems[1]) * 1000L);
            }
        });

        // 抽取 watermark 时间×
        SingleOutputStreamOperator<Tuple2<String, Long>> withWatermarkStream = mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 最大迟到时间 0 s
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        // 抽取事件中第二个元素为时间戳字段，必须为毫秒
                                        System.out.println("当前水位线为：" + element.f1);
                                        return element.f1;
                                    }
                                }
                        )

        );

        // 按第一个字段分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = withWatermarkStream.keyBy(e -> e.f0);

        // 为分组后的事件划分一个 5 s 的滚动事件时间窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        // 统计窗口中元素的个数
        SingleOutputStreamOperator<String> countWinProcessStream = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                Timestamp winStart = new Timestamp(context.window().getStart());
                Timestamp winEnd = new Timestamp(context.window().getEnd());

                int count = 0;

                Iterator<Tuple2<String, Long>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }

                out.collect("key = " + key + "\t window [ " + winStart + " - " + winEnd + " ) 有 " + count + " 条元素");

            }
        });

        // sink
        countWinProcessStream.print();

        // 执行
        env.execute();

    }


}
