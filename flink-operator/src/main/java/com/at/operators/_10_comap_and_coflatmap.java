package com.at.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-14
 */
public class _10_comap_and_coflatmap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataStreamSource<Tuple2<String, Integer>> bjStream = env
                .fromElements(
                        Tuple2.of("BJ", 80),
                        Tuple2.of("SH", 167)
                );

        DataStreamSource<Tuple2<String, Integer>> shStream = env
                .fromElements(
                        Tuple2.of("SH", 180),
                        Tuple2.of("BJ", 100)
                );

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStreams = bjStream
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .connect(shStream.keyBy(sh -> sh.f0));


        connectedStreams
                .map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {

                    //map1处理来自第一条流的元素
                    @Override
                    public String map1(Tuple2<String, Integer> value) throws Exception {
                        return value.f0 + " 的平均年龄 " + value.f1 + " 岁";
                    }

                    //map2处理来自第二条流的元素
                    @Override
                    public String map2(Tuple2<String, Integer> value) throws Exception {
                        return value.f0 + " 的平均身高  " + value.f1 + " cm";
                    }
                })
                .print();

        connectedStreams
                .flatMap(new CoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void flatMap1(Tuple2<String, Integer> value, Collector<String> out) throws Exception {
                        out.collect(value.f0 + " 的平均年龄 " + value.f1 + " 岁");
                    }

                    @Override
                    public void flatMap2(Tuple2<String, Integer> value, Collector<String> out) throws Exception {
                        out.collect(value.f0 + " 的平均身高  " + value.f1 + " cm");
                        out.collect(value.f0 + " 的平均身高  " + value.f1 + " cm");
                    }
                })
                .print();


        env.execute();

    }

}
