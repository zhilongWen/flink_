package com.at;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
public class _4_keyby {

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

        keyedStream.print();

        env.execute();

    }

}
