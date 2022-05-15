package com.at.operator;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-15
 */
public class FlatMapOperator {

    //  Mary 的 点击行为 复制 2 次
    // Alice 的 点击行为 复制 1 次
    // 其余过滤掉

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        // DataStream → DataStream
        // flatMap：针对流中的每一个元素，输出零个、一个或多个 元素
        // flatMap 是 map 与 filter 的泛化  可以实现 map 与 filter 的功能

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<Event, Event>() {
                    @Override
                    public void flatMap(Event value, Collector<Event> out) throws Exception {
                        if ("Mary".equals(value.user)) {
                            out.collect(value);
                            out.collect(value);
                        } else if ("Alice".equals(value.user)) {
                            out.collect(value);
                        }
                    }
                })
                .print();

        env.execute();
    }
}


