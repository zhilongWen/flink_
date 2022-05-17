package com.at.processfunction;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-17
 */
public class BroadcastProcessFunctions {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 点击流
        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        // 广播流
        DataStreamSource<Tuple2<String, Integer>> broadCastStream = env
                .fromElements(
                        Tuple2.of("Mary", 100),
                        Tuple2.of("Bob", 100),
                        Tuple2.of("Alice", 100)
                );


        env.execute();


    }

}
