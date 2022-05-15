package com.at.window;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.plugin.services.WNetscape4BrowserService;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Iterator;

/**
 * @create 2022-05-15
 */
public class TumblingWindows {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        /*
            Tumbling Windows 滚动窗口

                1.窗口不重叠
                2.触发窗口计算，并销毁窗口中的元素

         */

        env
                .addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
//                                .withTimestampAssigner(
//                                        new SerializableTimestampAssigner<Event>() {
//                                            @Override
//                                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                                return element.timestamp;
//                                            }
//                                        }
//                                )
//                )
                .keyBy(event -> event.user)
                // 窗口大小 5s
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))

                .process(
                        //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
                        new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());

                                int count = 0;

                                Iterator<Event> iterator = elements.iterator();
                                while (iterator.hasNext()) {
                                    iterator.next();
                                    count++;
                                }


                                out.collect("key = " + key + "\t window [ " + winStart + " - " + winEnd + " ] 有 " + count + "条元素");

                            }
                        })
                .print();

        env.execute();


    }

}
