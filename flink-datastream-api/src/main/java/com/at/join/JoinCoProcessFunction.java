package com.at.join;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-21
 */
public class JoinCoProcessFunction {

    /*

        使用 CoProcessFunction 实现等值内连接

            SELECT * FROM A INNER JOIN B WHERE A.id=B.id

     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> streamOne = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("b", 2),
                        Tuple2.of("a", 2)
                );

        DataStreamSource<Tuple2<String, String>> streamTwo = env
                .fromElements(
                        Tuple2.of("a", "s"),
                        Tuple2.of("b", "b"),
                        Tuple2.of("a", "aaa")
                );

        streamOne
                .keyBy(r -> r.f0)
                .connect(streamTwo.keyBy(r -> r.f0))
                .process(
                        new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>() {

                            private ListState<Tuple2<String, Integer>> listStateOne;
                            private ListState<Tuple2<String, String>> listStateTwo;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                listStateOne = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("list-state-one", Types.TUPLE(Types.STRING, Types.INT)));
                                listStateTwo = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, String>>("list-state-two", Types.TUPLE(Types.STRING, Types.STRING)));
                            }

                            @Override
                            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {

                                listStateOne.add(value);

                                for (Tuple2<String, String> elem : listStateTwo.get()) {
                                    out.collect(value + " -> " + elem);
                                }


                            }

                            @Override
                            public void processElement2(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {

                                listStateTwo.add(value);

                                for (Tuple2<String, Integer> elem : listStateOne.get()) {
                                    out.collect(value + " -> " + elem);
                                }

                            }
                        }
                )
                .print();


        env.execute();


    }

}
