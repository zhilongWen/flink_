package com.at;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @create 2022-05-12
 */
public class _5_reduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 1, 5));


        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.map(new MapFunction<Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Integer elem) throws Exception {
                if (elem == 1) {
                    return Tuple2.of("w", elem);
                } else {
                    return Tuple2.of("o", elem);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> elem) throws Exception {
                return elem.f0;
            }
        });

        // reduce
        keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });


        // min 在输入流上对指定的字段求最小值
        keyedStream.min(1).print();

        // minBy 在输入流上针对指定字段求最小值，并返回包含当前观察到的最小值的事件
        keyedStream.minBy(1);

        // max 在输入流上对指定的字段求最大值
        keyedStream.max(1);

        // 在输入流上针对指定字段求最大值，并返回包含当前观察到的最大值的事件
        keyedStream.maxBy(0);

        // sum 在输入流上对指定的字段做滚动相加操作
        keyedStream.sum(1);

        env.execute();

    }
}
