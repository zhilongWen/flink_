package com.at.window;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Iterator;

/**
 * @create 2022-05-16
 */
public class GlobalWindows {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*
            Global Window

                A global windows assigner assigns all elements with the same key to the same single global window.
                This windowing scheme is only useful if you also specify a custom trigger.
                Otherwise, no computation will be performed, as the global window does not have a natural end at which we could process the aggregated elements.
         */

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner(
//                                        new SerializableTimestampAssigner<Event>() {
//                                            @Override
//                                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                                return element.timestamp;
//                                            }
//                                        }
//                                )
//                )
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create())
                .trigger(
                        // 第一条元素 的 2s 后触发窗口计算
                        new Trigger<Event, GlobalWindow>() {


                            // 每来一条数据就会触发一次
                            @Override
                            public TriggerResult onElement(Event element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

                                // 定义一个状态变量 用来标记窗口的的第一个元素
                                ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));

                                if(firstSeen.value() == null ){

                                    // 窗口中的第一个元素

                                    //获取下一个最近整数秒

                                    System.out.println("第一条元素到了：" + new Timestamp(element.timestamp) + " -> " + element.timestamp + " -> " + new Timestamp((element.timestamp + 2 * 1000L)) + " -> "  + (element.timestamp + 2 * 1000L));

                                    long ts = element.timestamp  + 2 * 1000L;


                                    // 注册一个定时器
                                    ctx.registerProcessingTimeTimer(ts);

                                    //更新 firstSeen 状态变量
                                    firstSeen.update(true);

                                }


                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {

                                System.out.println("触发器执行了：" + new Timestamp(time) + " -> " + time);

                                ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));

                                // 清空状态变量
                                firstSeen.clear();

                                return TriggerResult.FIRE;
                            }

                            // 触发窗口
                            @Override
                            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {


                            }
                        })
                .process(
                        new ProcessAllWindowFunction<Event, String, GlobalWindow>() {
                            @Override
                            public void process(Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

//                                int count = 0;
//                                Iterator<Event> iterator = elements.iterator();
//                                while (iterator.hasNext()){
//                                    iterator.next();
//                                    count++;
//                                }

                                // 获取迭代器中的元素个数
                                long count = elements.spliterator().getExactSizeIfKnown();

                                out.collect("window [ " + new Timestamp(context.window().maxTimestamp()) + " ] 有 " + count + " 条元素");

                            }
                        }
                )
                .print();

        env.execute();

    }

}
