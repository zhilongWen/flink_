package com.at.processfunction;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * @create 2022-05-18
 */
public class ProcessAllWindowFunctions {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Event>() {

                            // 抽取流数据的那个字段为水位线字段
                            @Override
                            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                };
                            }

                            @Override
                            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                // 构造水位线
                                return new WatermarkGenerator<Event>() {

                                    // 最大延迟时间
                                    private Long delayTs = 5000L;

                                    // 系统观察到的最大时间
                                    private Long maxTs = -Long.MAX_VALUE + delayTs + 1L;

                                    @Override
                                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                                        maxTs = Math.max(maxTs,event.timestamp);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 发送水位线，注意水位线的计算公式
                                        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
                                    }
                                };
                            }
                        }
                )
                .windowAll(GlobalWindows.create())
                .trigger(
                        // 每 10s 触发一次窗口计算
                        new Trigger<Event, GlobalWindow>() {
                            @Override
                            public TriggerResult onElement(Event element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Boolean> tenSecTriggerState = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("ten-sec-trigger-state", Types.BOOLEAN));

                                if(!Optional.ofNullable(tenSecTriggerState.value()).orElseGet(() -> false)){

                                    ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + 10 * 1000L);

                                    tenSecTriggerState.update(true);

                                }

                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Boolean> tenSecTriggerState = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("ten-sec-trigger-state", Types.BOOLEAN));

                                tenSecTriggerState.clear();

                                return TriggerResult.FIRE;
                            }

                            @Override
                            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

                                System.out.println("============================");

//                                ValueState<Boolean> tenSecTriggerState = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("ten-sec-trigger-state", Types.BOOLEAN));
//
//                                tenSecTriggerState.clear();

                            }
                        }
                )
                .process(new ProcessAllWindowFunction<Event, String, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        out.collect("有 " + elements.spliterator().getExactSizeIfKnown() + " 条元素");
                    }
                })
                .print();


        env.execute();


    }

}
