package com.at.keyby;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class AGGReduceOperator {


    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        stream.keyBy(r -> r.f0).sum(1).print("sum：");
        stream.keyBy(r -> r.f0).sum("f1").print("sum -> f1：");
        stream.keyBy(r -> r.f0).max(1).print("max：");
        stream.keyBy(r -> r.f0).max("f1").print("max -> f1：");
        stream.keyBy(r -> r.f0).min(1).print("min：");
        stream.keyBy(r -> r.f0).min("f1").print("min -> f1：");
        stream.keyBy(r -> r.f0).maxBy(1).print("maxBy：");
        stream.keyBy(r -> r.f0).maxBy("f1").print("maxBy -> f1：");
        stream.keyBy(r -> r.f0).minBy(1).print("minBy：");
        stream.keyBy(r -> r.f0).minBy("f1").print("minBy -> f1：");

        env.execute();


    }


}
