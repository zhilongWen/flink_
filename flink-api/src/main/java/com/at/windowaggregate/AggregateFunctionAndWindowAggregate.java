package com.at.windowaggregate;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @create 2022-05-18
 */
public class AggregateFunctionAndWindowAggregate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 增量聚合 + 全窗口聚合

        env
                .addSource(new ClickSource())
                .keyBy(event -> event.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(

                        // 增量聚合  定义增量聚合累加规则
                        new AggregateFunction<Event, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Event value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        },

                        // 全窗口聚合 添加窗口信息
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {

                                Timestamp winS = new Timestamp(context.window().getStart());
                                Timestamp winE = new Timestamp(context.window().getEnd());

                                // 此时窗口中只有 一条数据 那就是累加器的值
                                long count = elements.iterator().next();

                                out.collect("key = " + key + "\twindow [ " + winS + " - " + winE + " ) 窗口中 pv =  " + count);

                            }
                        }

                )
                .print();

        env.execute();

    }

}
