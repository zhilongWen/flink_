package com.at.evictor;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Optional;

/**
 * @create 2022-05-19
 */
public class WindowEvictor {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        env
//                .addSource(new ClickSource())
//                .keyBy(event -> 1)
//                .countWindow(10,5)
//                .process(
//                        new ProcessWindowFunction<Event, String, Integer, GlobalWindow>() {
//                            @Override
//                            public void process(Integer integer, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
//                                out.collect("--- : " + elements.spliterator().getExactSizeIfKnown());
//                            }
//                        }
//                )
//                .print();


        env
                .addSource(new ClickSource())
                .keyBy(event -> 1)
                .windowAll(GlobalWindows.create())

                // 窗口 只 保存 20 条最近数据

                .trigger(

                        // 1min 触发一次
                        new Trigger<Event, GlobalWindow>() {
                            @Override
                            public TriggerResult onElement(Event element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));

                                if (!Optional.ofNullable(firstSeen.value()).orElseGet(() -> false)) {

                                    // 窗口的第一个元素

                                    //注册一个 1min 的定时器
//                                    System.out.println("timestamp = " + timestamp); // -9223372036854775808

                                    long ts = ctx.getCurrentProcessingTime() + 5 * 1000L;

                                    System.out.println("register timer = " + ts);

                                    ctx.registerProcessingTimeTimer(ts);

                                    firstSeen.update(true);

                                }

                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {

                                System.out.println("timer trigger = " + time);

                                ctx.registerProcessingTimeTimer(time + 1L + 5 * 1000L);

                                return TriggerResult.FIRE;
                            }

                            @Override
                            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
                                ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));
                                firstSeen.clear();
                            }
                        }
                )

                // org.apache.flink.streaming.api.windowing.evictors.CountEvictor
                .evictor(

                        // 保留最近 20 条数据

                        new Evictor<Event, GlobalWindow>() {

                            private int maxCount = 20;

                            @Override
                            public void evictBefore(Iterable<TimestampedValue<Event>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

                                // System.out.println("class = "+elements.getClass()); // class org.apache.flink.shaded.guava30.com.google.common.collect.Iterables$5

                                long total = elements.spliterator().getExactSizeIfKnown();

                                if (total < maxCount) {
                                    return;
                                } else {

                                    // 需要移除窗口元素的数量 evictedCount = total - maxCount
                                    int evictedCount = 0;

                                    for (Iterator<TimestampedValue<Event>> iterator = elements.iterator(); iterator.hasNext();) {

                                        iterator.next();

                                        evictedCount++;

                                        if (evictedCount > total - maxCount) {
                                            break;
                                        } else {
                                            iterator.remove();
                                        }

                                    }

                                }


                            }

                            @Override
                            public void evictAfter(Iterable<TimestampedValue<Event>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

                            }
                        }
                )
                .process(
                        new ProcessAllWindowFunction<Event, String, GlobalWindow>() {
                            @Override
                            public void process(Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                                out.collect("窗口中有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素");
                            }
                        }
                )
                .print();

        env.execute();


    }

}
