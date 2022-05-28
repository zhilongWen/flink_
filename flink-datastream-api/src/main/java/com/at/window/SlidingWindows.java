package com.at.window;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @create 2022-05-15
 */
public class SlidingWindows {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
            Sliding Windows 滚动窗口

                1.窗口重叠
                2.每隔滑动步长秒触发窗口计算，但会保留窗口部分元素
                3.下边界 移动到 上边界 是清空下边界的所有元素

         */

        env
                .addSource(new ClickSource())
                .keyBy(event -> event.user)
                // 滚动窗口 窗口大小 10s   滑动步长 5s
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

                        Timestamp winStart = new Timestamp(context.window().getStart());
                        Timestamp winEnd = new Timestamp(context.window().getEnd());

                        int count = 0;

                        Iterator<Event> iterator = elements.iterator();
                        while (iterator.hasNext()){
                            iterator.next();
                            count++;
                        }

                        String result = String.format("key = %s \t window [ %s - %s] 有 %d 条元素", key, winStart, winEnd, count);

                        out.collect(result);

                    }
                })
                .print();

        env.execute();


    }

}
