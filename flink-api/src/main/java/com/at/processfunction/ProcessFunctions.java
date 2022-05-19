package com.at.processfunction;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.EmptyStackException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @create 2022-05-16
 */
public class ProcessFunctions {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // ProcessFunction 不能使用 状态变量 不能使用 onTimer 编译会出错

        // 求平均值  实现 map - reduce

        DataStreamSource<Integer> sourceStream = env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(100));
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                });


        // 求平均值
        sourceStream
                .map(elem -> Tuple2.of(elem, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(elem -> true)
                .reduce((r1, r2) -> Tuple2.of(r1.f0 + r2.f0, r1.f1 + r2.f1))
                .map(elem -> (double) elem.f0 / elem.f1)
                .print("map-reduce:");


        sourceStream
                .keyBy(elem -> true)
                .process(
                        // 第一个泛型输入类型
                        // 第二个泛型输出泛型
                        new ProcessFunction<Integer, Double>() {

                            // 定义一个状态变量 用于统计并计数
                            private ValueState<Tuple2<Integer, Integer>> valueAndCountState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                valueAndCountState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Tuple2<Integer, Integer>>("value-and-count-state", Types.TUPLE(Types.INT, Types.INT))
                                );
                            }

                            // 没来一条数据都会调用 processElement 方法
                            @Override
                            public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {

                                if (valueAndCountState.value() == null) {
                                    valueAndCountState.update(Tuple2.of(value, 1));
                                } else {
                                    valueAndCountState.update(Tuple2.of(valueAndCountState.value().f0 + value, valueAndCountState.value().f1 + 1));
                                }

                                out.collect((double) valueAndCountState.value().f0 / valueAndCountState.value().f1);

                            }
                        })
                .print("processfunction:");


        env.execute();

    }

}
