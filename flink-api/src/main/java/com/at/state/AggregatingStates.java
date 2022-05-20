package com.at.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @create 2022-05-21
 */
public class AggregatingStates {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*
            AggregatingState<IN, OUT>
                This keeps a single value that represents the aggregation of all values added to the state.
                Contrary to ReducingState, the aggregate type may be different from the type of elements that are added to the state.
                The interface is the same as for ListState but elements added using add(IN) are aggregated using a specified AggregateFunction.
         */


        env
                .addSource(new SourceFunction<Tuple2<String, Integer>>() {

                    private List<Character> letters = new ArrayList<>(Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'));

                    private Random random = new Random();

                    private boolean running = true;

                    @Override
                    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {

                        while (running) {

                            ctx.collect(Tuple2.of(String.valueOf(letters.get(random.nextInt(26))), random.nextInt(30)));

                            Thread.sleep(10);

                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(e -> e.f0)
                .flatMap(
                        new RichFlatMapFunction<Tuple2<String, Integer>, String>() {

                            private int count = 0;

                            private transient AggregatingState<Tuple2<String, Integer>, Integer> aggState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                AggregatingStateDescriptor<Tuple2<String, Integer>, Integer, Integer> aggStateDescriptor
                                        = new AggregatingStateDescriptor<>(
                                        "agg-state",
                                        new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                                            @Override
                                            public Integer createAccumulator() {
                                                return 0;
                                            }

                                            @Override
                                            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                                                return accumulator + value.f1;
                                            }

                                            @Override
                                            public Integer getResult(Integer accumulator) {
                                                return accumulator;
                                            }

                                            @Override
                                            public Integer merge(Integer a, Integer b) {
                                                return a + b;
                                            }
                                        },
                                        Types.INT);

                                aggState = getRuntimeContext().getAggregatingState(aggStateDescriptor);


                            }

                            @Override
                            public void flatMap(Tuple2<String, Integer> value, Collector<String> out) throws Exception {

                                count++;

                                if (count % 50 == 0) {
                                    out.collect( "key = " + value.f0 + "\tcount = " + aggState.get());
                                    aggState.clear();
                                } else {
                                    // 增量更新 AggregatingState ，每来一条元素 acc + f1
                                    aggState.add(value);
                                }


                            }
                        }
                )
                .print();

        env.execute();

    }

}
