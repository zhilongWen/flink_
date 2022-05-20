package com.at.state;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @create 2022-05-21
 */
public class ListStates {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        // 使用 ListState 实现 sliding countWindow

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(e -> true)
                .process(
                        new KeyedProcessFunction<Boolean, Event, String>() {

                            private ListState<Event> windowAllElement;

                            private final Integer COUNT = 50;
                            private final Integer SLIDE = 20;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                windowAllElement = getRuntimeContext().getListState(new ListStateDescriptor<Event>("window-all-element", Types.POJO(Event.class)));
                            }

                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                                windowAllElement.add(value);

                                long count = windowAllElement.get().spliterator().getExactSizeIfKnown();

                                if (count % SLIDE == 0) {
                                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1L);
                                }

                                if (count == COUNT) {

                                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1L);

                                }

                            }


                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);

                                long count = windowAllElement.get().spliterator().getExactSizeIfKnown();
                                if (count % SLIDE == 0) {


                                    // 20
                                    out.collect("20 - > 窗口中有 --> 1 " + count + " 条数据");

                                    if (count > COUNT) {

                                        // 移除多余的元素

                                        int evictorCount = 0;

                                        for (Iterator<Event> iterator = windowAllElement.get().iterator(); iterator.hasNext(); ) {

                                            Event next = iterator.next();
                                            evictorCount++;

                                            if (evictorCount > count - COUNT) {
                                                break;
                                            } else {
                                                System.out.println("remove: " + next);
                                                iterator.remove();
                                            }

                                        }


                                    }


                                    out.collect("20 - > 窗口中有 --> 1 " + windowAllElement.get().spliterator().getExactSizeIfKnown() + " 条数据");


                                } else {

                                    // 50
                                    out.collect("50 窗口中有 " + count + " 条数据");

                                }


                            }
                        }
                )
                .print();


        env.execute();


    }

}
