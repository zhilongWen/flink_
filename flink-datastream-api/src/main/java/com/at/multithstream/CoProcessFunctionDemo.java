package com.at.multithstream;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2023-03-01
 */
public class CoProcessFunctionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> clickStream = env
                .addSource(new ClickSource());
//                .filter(r -> "./home".equals(r.url) || "./fav".equals(r.url));

        // ./home 放行 5 s
        // ./fav 放行 10 s
        DataStreamSource<Tuple2<String, Long>> switchStream = env
                .fromElements(
                        Tuple2.of("./home", 5 * 1000L),
                        Tuple2.of("./fav", 15 * 1000L)
                );

        clickStream
                .connect(switchStream)
                .keyBy(cR -> cR.url,rR -> rR.f0)
                .process(
                        new CoProcessFunction<Event, Tuple2<String, Long>, Event>() {

                            private ValueState<Boolean> enableThroughTs ;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                enableThroughTs = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("enableThroughTs", Types.BOOLEAN));
                            }

                            @Override
                            public void processElement1(Event value, Context ctx, Collector<Event> out) throws Exception {

                                if (enableThroughTs.value() != null && enableThroughTs.value()){
                                    out.collect(value);
                                }

                            }

                            @Override
                            public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<Event> out) throws Exception {

                                // 处理开关流

                                // 打开开关
                                enableThroughTs.update(true);

                                // 注册定时器关闭开关
                                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);

                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {

                                // 关闭开关
                                enableThroughTs.clear();

                            }
                        }
                )
                .print();



        env.execute();


    }

}
