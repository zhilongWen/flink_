package com.at.join;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-21
 */
public class JoinConnect {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<String> ruleStream = env.socketTextStream("127.0.0.1", 9099);

        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());


        // connect  联结两条流
        // 1. 只能连结两条流
        // 2. 两条流中元素类型可以不同

//        clickStream
//                .connect(ruleStream)
//                .flatMap(
//                        new CoFlatMapFunction<Event, String, Event>() {
//
//                            private String rule = "";
//
//                            @Override
//                            public void flatMap1(Event value, Collector<Event> out) throws Exception {
//
//                                if (value.user.equals(rule)) out.collect(value);
//
//                            }
//
//                            @Override
//                            public void flatMap2(String value, Collector<Event> out) throws Exception {
//                                rule = value;
//                            }
//                        }
//                )
//                .print();


        clickStream
                .keyBy(r -> r.user)
                .connect(ruleStream.broadcast())
                .flatMap(
                        new CoFlatMapFunction<Event, String, Event>() {

                            private String rule = "";

                            @Override
                            public void flatMap1(Event value, Collector<Event> out) throws Exception {
                                if (value.url.equals(rule)) out.collect(value);
                            }

                            @Override
                            public void flatMap2(String value, Collector<Event> out) throws Exception {
                                rule = value;
                            }
                        }
                )
                .print();


        env.execute();


    }

}
