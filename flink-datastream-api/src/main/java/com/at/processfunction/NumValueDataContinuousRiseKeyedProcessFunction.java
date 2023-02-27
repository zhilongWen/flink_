package com.at.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @create 2023-02-23
 */
public class NumValueDataContinuousRiseKeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        // 整数连续 1s 上升报警

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> randomNumSourceStream = env
                .addSource(new SourceFunction<Integer>() {

                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(100000));

                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                        random = null;
                    }
                });

        randomNumSourceStream
                // 将所有数据分到一个 slot
                .keyBy(r -> 1)
                // KEY、INPUT、OUTPUT
                .process(new KeyedProcessFunction<Integer, Integer, String>() {

                    // 保存上一条数据
                    private ValueState<Integer> lastValueState;

                    // 保存定时器
                    private ValueState<Long> timerTs;

                    private final Long oneS = 1000L;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastValueState", Types.INT));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Types.LONG));
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {

                        // =====================
                        Integer lastVal = lastValueState.value() == null ? Integer.MIN_VALUE : lastValueState.value();

                        // 跟新上一个元素
                        lastValueState.update(value);

                        Long ts = null;
                        if (timerTs.value() != null){
                            ts = timerTs.value();
                        }

                        if (lastVal >= value){
                            if (ts != null){
                                System.out.println("当前元素小与上一条元素,curr = " + value + ", last = " + lastVal + ", 删除定时器 = " + ts);
                                ctx.timerService().deleteProcessingTimeTimer(ts);
                                timerTs.clear();
                            }
                        }else {
                            System.out.println("当前元素大于上一条元素 -->>>，curr = " + value + ", last = " + lastVal);
                            if (ts == null){
                                long timer = ctx.timerService().currentProcessingTime() + oneS;
                                System.out.println("当前元素大于上一条，并且是第一条元素，curr = " + value + ", last = " + lastVal + ", 注册定时器 = " + timer);
                                ctx.timerService().registerProcessingTimeTimer(timer);
                                timerTs.update(timer);
                            }

                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("整数连续一秒上升");
                        timerTs.clear();
                    }
                })
                .print();


        env.execute();


    }

}
