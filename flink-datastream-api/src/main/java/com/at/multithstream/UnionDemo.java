package com.at.multithstream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2023-03-01
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Integer> streamOne = env.fromElements(1, 2);

        DataStreamSource<Integer> streamTwo = env.fromElements(3, 4);

        DataStreamSource<Integer> streamThree = env.fromElements(5, 6);


        // union
        // 1. 多条流的合并
        // 2. 所有流中的事件类型必须是一样的
        // 先来先处理

        DataStream<Integer> unionStream = streamOne.union(streamTwo, streamThree);

        unionStream.print();


        env.execute();
    }
}
