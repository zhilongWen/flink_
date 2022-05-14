package com.at;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @create 2022-05-12
 */
public class _1_map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        // map: 针对流中的每一个元素，输出一个元素
        SingleOutputStreamOperator<Integer> streamOperator = streamSource
                // 输入泛型：Integer; 输出泛型：Integer
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer i) throws Exception {
                        return i + 10;
                    }
                });

        streamOperator.print();

        env.execute();

    }

}
