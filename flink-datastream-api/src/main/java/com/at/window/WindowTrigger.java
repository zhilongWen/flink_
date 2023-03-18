package com.at.window;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-19
 */
public class WindowTrigger {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env
                .addSource(new ClickSource())
                .keyBy(event -> true)
                .countWindow(50)
                .trigger(
                        // 每 20 条数据触发一次窗口计算
                        // 窗口结束 触发窗口计算并清空窗口
                        new Trigger<Event, GlobalWindow>() {
                            @Override
                            public TriggerResult onElement(Event element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Integer> countState = ctx.getPartitionedState(new ValueStateDescriptor<Integer>("count-state", Types.INT));

                                if(countState.value() == null){
                                    countState.update(1);
                                }else {
                                    countState.update(countState.value() + 1);
                                }

                                if(countState.value() % 20 == 0){
                                    // 注册 20条数据的 一个定时器
                                    ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + 1L);
                                }

                                if(countState.value() == 50){
                                    // 注册一个 窗口结束的定时器
                                    ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + 1L);
                                }

                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Integer> countState = ctx.getPartitionedState(new ValueStateDescriptor<Integer>("count-state", Types.INT));

                                if(countState.value() % 20 ==0){
                                    return TriggerResult.FIRE;
                                }else {
                                    countState.clear();
                                    return TriggerResult.FIRE_AND_PURGE;
                                }

                            }

                            @Override
                            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
//                                ValueState<Integer> countState = ctx.getPartitionedState(new ValueStateDescriptor<Integer>("count-state", Types.INT));
//                                countState.clear();
                            }
                        }
                )
                .process(
                        new ProcessWindowFunction<Event, String, Boolean, GlobalWindow>() {
                            @Override
                            public void process(Boolean s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                                out.collect("窗口中有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素");
                            }
                        }
                )
                .print();


        env.execute();


    }

}
