package com.at.operator;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class FilterOperator {

    // 过滤 Mary 的 点击行为

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //filter: 针对流中的每一个元素，输出零个或一个元素

        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "Mary".equals(value.user);
                    }
                })
                .print();

        env.execute();
    }
}
