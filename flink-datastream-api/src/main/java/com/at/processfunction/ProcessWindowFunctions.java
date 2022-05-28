package com.at.processfunction;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Optional;

/**
 * @create 2022-05-17
 */
public class ProcessWindowFunctions {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(
                        // 整数秒触发窗口统计
                        // 窗口结束触发窗口统计并销毁窗口
                        new Trigger<Event, TimeWindow>() {

                            // 没来一条数据都会调用该方法
                            @Override
                            public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Boolean> winFirstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("window-first-seen", Types.BOOLEAN));

                                if (!Optional.ofNullable(winFirstSeen.value()).orElseGet(() -> false)) {

                                    // 窗口的第一条元素

                                    // 注册一个水位线 下 一个 整数秒 定时器
                                    ctx.registerEventTimeTimer(ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000l));

                                    // 注册一个窗口结束时的定时器
                                    ctx.registerEventTimeTimer(window.getEnd());

                                    winFirstSeen.update(true);


                                }

                                return TriggerResult.CONTINUE;
                            }

                            // 机器时间
                            @Override
                            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            // 事件时间
                            @Override
                            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

                                if (time == window.getEnd()) {

                                    // 窗口结束时的触发规则
                                    return TriggerResult.FIRE_AND_PURGE;

                                }

                                // 整数秒的触发规则


                                // 注册一个水位线 下 一个 整数秒 定时器
                                // 注册窗口的时间不能大于窗口的结束时间
                                long lastOneSecTimer = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000l);
                                if (lastOneSecTimer < window.getEnd()) ctx.registerEventTimeTimer(lastOneSecTimer);


                                return TriggerResult.FIRE;
                            }

                            @Override
                            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                                System.out.println("window [ " + new Timestamp(window.getStart()) + " - " + new Timestamp(window.getEnd()) + " ) 窗口触发计算");

                                ValueState<Boolean> winFirstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("window-first-seen", Types.BOOLEAN));

                                // 清空状态变量
                                winFirstSeen.clear();

                            }
                        }
                )
                .process(
                        // ProcessWindowFunction<IN, OUT, KEY, W extends Window>
                        new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

                                Timestamp winStart = new Timestamp(context.window().getStart());
                                Timestamp winEnd = new Timestamp(context.window().getEnd());
                                long count = elements.spliterator().getExactSizeIfKnown();

                                out.collect("key = " + key + "\twindow [ " + winStart + " - " + winEnd + " ) 的窗口中有 " + count + " 条元素");
                            }
                        }
                )
                .print();


        env.execute();


    }

}
