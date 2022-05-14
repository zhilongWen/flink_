package com.at;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-14
 */
public class _9_union {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataStreamSource<Tuple2<String, Integer>> bjStream = env
                .fromElements(
                        Tuple2.of("BJ", 111),
                        Tuple2.of("BJ", 222),
                        Tuple2.of("BJ", 333)
                        );

        DataStreamSource<Tuple2<String, Integer>> shStream = env
                .fromElements(
                        Tuple2.of("SH", 123),
                        Tuple2.of("SH", 234),
                        Tuple2.of("SH", 345)
                );


        DataStreamSource<Tuple2<String, Integer>> gzStream = env
                .fromElements(
                        Tuple2.of("GZ", 12),
                        Tuple2.of("GZ", 13),
                        Tuple2.of("GZ", 14)
                );

        //将两个或多个数据流联合来创建一个包含所有流中数据的新流
        DataStream<Tuple2<String, Integer>> unionStream = bjStream.union(shStream, gzStream);


        unionStream.print();


        env.execute();

    }

}
