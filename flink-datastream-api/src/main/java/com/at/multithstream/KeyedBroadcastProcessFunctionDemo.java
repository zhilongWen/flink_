package com.at.multithstream;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-17
 */
public class KeyedBroadcastProcessFunctionDemo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 点击流
        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        // 广播流
        DataStreamSource<Tuple2<String, Integer>> infoStream = env
                .fromElements(
                        Tuple2.of("Mary", 100),
                        Tuple2.of("Bob", 100),
                        Tuple2.of("Alice", 100)
                );

        // 配置广播流
        MapStateDescriptor<String, Tuple2<String, Integer>> broadCastDescriptor = new MapStateDescriptor<String, Tuple2<String, Integer>>("broad-test", Types.STRING, Types.TUPLE(Types.STRING, Types.INT));
        BroadcastStream<Tuple2<String, Integer>> broadcastStream = infoStream.broadcast(broadCastDescriptor);


        clickStream
                .keyBy(elem -> true)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Object, Event, Tuple2<String, Integer>, String>() {
                    @Override
                    public void processElement(Event event, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        ReadOnlyBroadcastState<String, Tuple2<String, Integer>> broadcastState = ctx.getBroadcastState(broadCastDescriptor);

                        out.collect(event + " -> " + broadcastState.get(event.user));

                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        // 设置广播流
                        BroadcastState<String, Tuple2<String, Integer>> broadcastState = ctx.getBroadcastState(broadCastDescriptor);

                        broadcastState.put(value.f0, value);

                    }
                })
                .print();


        env.execute();


    }


}
