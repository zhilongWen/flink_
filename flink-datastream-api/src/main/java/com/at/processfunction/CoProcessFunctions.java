package com.at.processfunction;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-17
 */
public class CoProcessFunctions {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将与 开关流 匹配的 点击流 数据向下发送 xs


        // 点击流
        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        // 开关流
        DataStreamSource<Tuple2<String, Long>> switchStream = env.fromElements(Tuple2.of("Mary", 20 * 1000L), Tuple2.of("Liz", 8 * 1000L));


        clickStream
                .connect(switchStream)
                .keyBy(r1 -> r1.user, r2 -> r2.f0)
                .process(
                        //CoProcessFunction<IN1, IN2, OUT>
                        new CoProcessFunction<Event, Tuple2<String, Long>, Event>() {

                            private ValueState<Boolean> forwardingEnable;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                forwardingEnable = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("forwarding-enable", Types.BOOLEAN));
                            }

                            // 处理来自点击流的数据
                            @Override
                            public void processElement1(Event value, Context ctx, Collector<Event> out) throws Exception {
                                // 如果开关是true，就允许数据流向下发送
                                if (forwardingEnable.value() != null && forwardingEnable.value()) {
                                    out.collect(value);
                                }
                            }

                            // 处理来自快关流的元素
                            @Override
                            public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<Event> out) throws Exception {
                                // 打开开关
                                forwardingEnable.update(true);

                                // 注册定时器
                                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);

                            }


                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                forwardingEnable.clear();
                            }
                        }
                )
                .print();


        env.execute();


    }

}
