package com.at.operator;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class MapOperator {

    // 提取出 Event user

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //map: 针对流中的每一个元素，输出一个元素

        env
                .addSource(new ClickSource())
                // 第一个泛型：输入类型，流中数据类型
                // 第二个泛型：输出类型
                .map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.user;
                    }
                })
                .print();

        env.execute();
    }

}
