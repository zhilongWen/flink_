package com.at.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @create 2022-05-17
 */
public class KeyedProcessFunctions {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        // 整数连续 1s 上升报警

        env
                .addSource(new SourceFunction<Integer>() {

                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {

                        while (running){

                            ctx.collect(random.nextInt(100));
                            Thread.sleep(100);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(elem -> 1)
                .process(
                        // KeyedProcessFunction<K, I, O>
                        new KeyedProcessFunction<Integer, Integer, String>() {

                            // 声明一个状态变量用于保存上一个正式值
                            // 状态变量的可见范围是当前key
                            // 状态变量是单例，只能被实例化一次
                            private ValueState<Integer> lastInt;

                            // 声明一个状态变量用于保存定时器
                            private ValueState<Long> timerTs;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                lastInt = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last-int", Types.INT));
                                timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Types.LONG));
                            }

                            // 流中的每一个元素都会调用该个方法
                            @Override
                            public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {

                                Integer lastVal = 0;
                                if(lastInt.value() == null){
                                    // 第一个元素
                                    lastVal = value;
                                }
                                // 更新上一个一个元素
                                lastInt.update(value);


                                Long timer = null;
                                if(timerTs.value() != null){
                                    timer = timerTs.value();
                                }

                                if(lastVal >= value){
                                    if(timer != null){
                                        // 来的元素比上上一条元素小 并且 当前定时器不为空 清空定时器
                                        ctx.timerService().deleteProcessingTimeTimer(timer);
                                    }
                                }else {

                                    // 当前元素 大于 上一条元素
                                    if (timer == null){
                                        // 并且当前定时器为空
                                        // 注册一个 1s 之后的定时器
                                        long oneSecLaterTimer = ctx.timerService().currentProcessingTime() + 1000L;
                                        ctx.timerService().registerProcessingTimeTimer(oneSecLaterTimer);
                                        // 更新定时器
                                        timerTs.update(oneSecLaterTimer);
                                    }

                                }


                            }

                            // 当之前注册的定时器触发时调用
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);

                                out.collect("整数连续 1s 上升");
                                timerTs.clear();

                            }
                        }
                )
                .print();


        env.execute();

    }

}
