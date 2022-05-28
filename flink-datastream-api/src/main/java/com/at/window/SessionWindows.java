package com.at.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @create 2022-05-15
 */
public class SessionWindows {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /*

            Session Windows  会话窗口

                1.指定时间内没有数据到来触发窗口计算并清空窗口中元素


                nc -lk 9099
                q
                w
                e
                r
                t
                y
                [waiting at least 5s]
                1
                w
                e
                r
                t
                ...

         */

        env
                .socketTextStream("127.0.0.1", 9099)
                .keyBy(event -> 1)
                // 5s 没有数据到来就触发窗口
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(
                        // ProcessWindowFunction<IN, OUT, KEY, W extends Window>
                        new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());

                                int count = 0;

                                Iterator<String> iterator = elements.iterator();
                                while (iterator.hasNext()) {
                                    iterator.next();
                                    count++;
                                }

                                String result = String.format("window [ %s - %s ] 窗口中有 %d 条元素", winStart, winEnd, count);

                                out.collect(result);

                            }
                        })
                .print();


        env.execute();


    }

}
