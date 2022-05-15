package com.at.keyby;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class ReduceOperator {



    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env
                .fromElements(
                        Tuple2.of("Alick",98),
                        Tuple2.of("Alick",69),
                        Tuple2.of("Bob",91),
                        Tuple2.of("Tom",87),
                        Tuple2.of("Bob",69)
                )
                .keyBy(elem -> elem.f1)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1 + value2.f1);
                    }
                })
                .print();



        env.execute();


    }

}
