package com.at.processfunction;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * @create 2023-02-26
 */
public class WinElemsCountProcessAllWindowFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<Event> withWatermarkStream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Event>() {
                            @Override
                            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Event>() {

                                    private long delay = 0L;

                                    private long maxWatermark = -Long.MAX_VALUE + delay + 1L;

                                    @Override
                                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                                        maxWatermark = Math.max(event.timestamp, maxWatermark);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        output.emitWatermark(new Watermark(maxWatermark - delay - 1L));
                                    }
                                };

                            }

                            @Override
                            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new TimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                };
                            }
                        }
                );

        AllWindowedStream<Event, GlobalWindow> allWindowedStream = withWatermarkStream
                .windowAll(GlobalWindows.create());

        SingleOutputStreamOperator<String> countAlWinElemsStream = allWindowedStream
                .trigger(
                        // 每 5 s 触发一次窗口计算
                        new Trigger<Event, GlobalWindow>() {

                            public final Long tenS = 5_000L;

                            @Override
                            public TriggerResult onElement(Event element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ValueState<Long> tenSTriggerState = ctx.getPartitionedState(new ValueStateDescriptor<Long>("tenSTriggerState", Types.LONG));

                                if (tenSTriggerState.value() == null) {
                                    long timer = ctx.getCurrentWatermark() + tenS;
                                    ctx.registerEventTimeTimer(timer);
                                    tenSTriggerState.update(timer);
                                }

                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                ValueState<Long> tenSTriggerState = ctx.getPartitionedState(new ValueStateDescriptor<Long>("tenSTriggerState", Types.LONG));

                                tenSTriggerState.clear();

                                return TriggerResult.FIRE;
                            }

                            @Override
                            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
                                System.out.println("=============================");
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
                );

        countAlWinElemsStream.print();


        env.execute();


    }

}
