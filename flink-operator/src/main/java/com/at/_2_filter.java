package com.at;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;


/**
 * @create 2022-05-12
 */
public class _2_filter {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        // filter  针对流中的每一个元素，输出零个或一个元素
        SingleOutputStreamOperator<Integer> streamOperator = streamSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer i) throws Exception {
                return i != 1;
            }
        });

        streamOperator.print();

        env.execute();

    }

}
