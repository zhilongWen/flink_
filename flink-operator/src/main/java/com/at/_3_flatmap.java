package com.at;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @create 2022-05-12
 */
public class _3_flatmap {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        SingleOutputStreamOperator<Integer> streamOperator = streamSource.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer elem, Collector<Integer> collector) throws Exception {

                if (elem == 1) {
                    collector.collect(elem);
                } else {
                    collector.collect(elem + 10);
                }
            }
        });

        streamOperator.print();

        env.execute();

    }

}
