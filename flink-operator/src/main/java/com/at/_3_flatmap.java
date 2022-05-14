package com.at;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @create 2022-05-12
 */
public class _3_flatmap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        // DataStream → DataStream
        // flatMap：针对流中的每一个元素，输出零个、一个或多个 元素
        // flatMap 是 map 与 filter 的泛化  可以实现 map 与 filter 的功能
//        streamSource.flatMap(new FlatMapFunction<Integer, Integer>() {
//            @Override
//            public void flatMap(Integer elem, Collector<Integer> collector) throws Exception {
//
//                if (elem % 2 == 1) {
//                    collector.collect(elem);
//                    collector.collect(elem);
//                } else {
//                    collector.collect(elem + 10);
//                }
//            }
//        }).print();


        streamSource
                .flatMap((Integer elem, Collector<Integer> collector) -> {
                    if (elem % 2 == 1) {
                        collector.collect(elem);
                        collector.collect(elem);
                    } else if (elem % 3 == 1) {
                        collector.collect(elem + 10);
                    }
                })
                .returns(Types.INT)
                .print();


        env.execute();

    }

}
