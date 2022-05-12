package com.at;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-12
 */
public class _3_flatmap {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1", 8090);

        SingleOutputStreamOperator<String> streamOperator = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {

                String[] elems = s.split(" ");

                for (String elem : elems) {

                    if (!elem.equals("a")) {
                        collector.collect(elem + " Flink");
                    }
                }
            }
        });

        streamOperator.print();

        env.execute();

    }

}
