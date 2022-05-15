package com.at.window;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Iterator;

/**
 * @create 2022-05-15
 */
public class CountWindows {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*
            Count Window 计数窗口

                窗口指定条数触发窗口计算，并清除前滑动步长个元素，滚动窗口步长等于窗口大小



         */

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(event -> true)
                // 滚动窗口
//                .countWindow(5)
                // 滑动动窗口
                .countWindow(10, 5)
                .process(new ProcessWindowFunction<Event, String, Boolean, GlobalWindow>() {
                    @Override
                    public void process(Boolean key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {


                        Timestamp win = new Timestamp(context.currentWatermark());

                        int count = 0;
                        Iterator<Event> iterator = elements.iterator();

                        while (iterator.hasNext()) {
                            iterator.next();
                            count++;
                        }

                        String result = String.format("window [ %s ] 窗口中有 %d 条元素", win, count);

                        out.collect(result);

                    }
                })
                .print();


        env.execute();


    }

}
