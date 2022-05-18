package com.at.windowaggregate;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @create 2022-05-18
 */
public class WindowAggregateFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 使用 增量聚合函数 统计每个用户 5s 中窗口的 pv

        env
                .addSource(new ClickSource())
                .keyBy(event -> event.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(
                        // AggregateFunction<IN, ACC, OUT>
                        new AggregateFunction<Event, Long, Long>() {
                            // 创建累加器
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            // 定义累加规则
                            @Override
                            public Long add(Event value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            // 窗口关闭时返回累加结果
                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            // 两个累加器合并规则
                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        }
                )
                .print();


        env.execute();

    }

}
