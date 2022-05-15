package com.at.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
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


        // 实现 filter 功能 过滤出奇数
        streamSource
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        if (value % 2 == 1) {
                            out.collect(value);
                        }
                    }
                })
                .print();

        // 实现 map 功能 求平方
        streamSource
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value * value);
                    }
                })
                .print();


        // 组合
        streamSource
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        if (value % 2 == 1) {
                            out.collect(value);
                        } else if (value % 3 == 0) {
                            out.collect(value);
                            out.collect(value);
                        }
                    }
                })
                .print();

        streamSource
                // 泛型擦除
                .flatMap((Integer value, Collector<Integer> out) -> {
                    if (value % 2 == 1) {
                        out.collect(value);
                    } else if (value % 3 == 0) {
                        out.collect(value);
                        out.collect(value);
                    }
                })
                .returns(Types.INT) // 显示指定类型
                .print();


        env.execute();

    }

}
