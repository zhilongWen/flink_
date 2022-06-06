package com.at;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @create 2022-05-13
 */
public class WC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 8099);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//
//                Arrays.stream(s.split(" ")).forEach(elem -> collector.collect(Tuple2.of(elem,1)));
//
//            }
//        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            private transient Counter inCounter;
            private transient Counter outCounter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                inCounter = getRuntimeContext()
                        .getMetricGroup()
                        .counter("MyInCounter");

                outCounter = getRuntimeContext()
                        .getMetricGroup()
                        .counter("MyOutCounter");
            }

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
//                inCounter.inc();
                Arrays.stream(s.split(" ")).forEach(elem -> {
//                    outCounter.inc();
                    out.collect(Tuple2.of(elem, 1));
                });
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> elem) throws Exception {
                return elem.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });

        reduceStream.print("res：");

        env.execute();

    }

}
