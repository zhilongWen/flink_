package com.at.state;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @create 2022-05-21
 */
public class BroadcastStates {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Tuple2<String, Integer>> ruleStream = env
                .fromElements(
                        Tuple2.of("Mary", 100),
                        Tuple2.of("Bob", 98),
                        Tuple2.of("other", 0)

                );


        MapStateDescriptor<String, Tuple2<String, Integer>> ruleStateDescriptor = new MapStateDescriptor<>(
                "rule-state",
                BasicTypeInfo.STRING_TYPE_INFO,
                Types.TUPLE(Types.STRING, Types.INT)
        );

        BroadcastStream<Tuple2<String, Integer>> broadcastStream = ruleStream.broadcast(ruleStateDescriptor);


        /// ==========================


        /*
            非广播流的类型
                如果流是一个 keyed 流，那就是 KeyedBroadcastProcessFunction 类型
                如果流是一个 non-keyed 流，那就是 BroadcastProcessFunction 类型
         */


        KeyedStream<Event, String> clickKeyStream = env.addSource(new ClickSource()).keyBy(event -> event.user);

        SingleOutputStreamOperator<String> result = clickKeyStream
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Event, Tuple2<String, Integer>, String>() {
                    @Override
                    public void processElement(Event event, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        Iterable<Map.Entry<String, Tuple2<String, Integer>>> entryIterable = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries();

                        for (Map.Entry<String, Tuple2<String, Integer>> entry : entryIterable) {

                            String key = entry.getKey();
                            Tuple2<String, Integer> val = entry.getValue();


                            if (key.equals(event.user)) {
                                out.collect(event + " -> " + val);
                            } else {
                                out.collect(event + " -> -> " + val);
                            }

                        }

                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {

                        ctx.getBroadcastState(ruleStateDescriptor).put(value.f0, value);

                    }
                });


        result.print();


        env.execute();


    }

}
