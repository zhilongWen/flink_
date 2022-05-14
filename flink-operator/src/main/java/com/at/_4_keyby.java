package com.at;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-14
 */
public class _4_keyby {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, Integer>> streamSource = env.fromElements(
                Tuple2.of(1, 2),
                Tuple2.of(1, 4)
        );


        // keyBy：将相同 key 的 数据分发到同一逻辑分区
        KeyedStream<Tuple2<Integer, Integer>, Integer> keyedStream = streamSource.keyBy(f -> f.f0);


        // sum 是 reduce 的实现
        keyedStream.sum(1).print();


        keyedStream.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        }).print();


        env.execute();

    }

}
