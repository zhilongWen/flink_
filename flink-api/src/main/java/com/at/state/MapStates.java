package com.at.state;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @create 2022-05-21
 */
public class MapStates {

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
                .process(

                        // 每 10s 统计一下 每个用户 每个 页面的点击 pv

                        new KeyedProcessFunction<String, Event, String>() {

                            private ValueState<Long> timerT;
                            private MapState<String, Long> avgPagePv;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                timerT = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Types.LONG));
                                avgPagePv = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("avg-page-pv", Types.STRING, Types.LONG));
                            }

                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                                if (!avgPagePv.contains(value.url)) {
                                    avgPagePv.put(value.url, 1L);
                                } else {
                                    avgPagePv.put(value.url, avgPagePv.get(value.url));
                                }

                                if (timerT.value() == null) {
                                    long ts = value.timestamp + 10 * 1000L;
                                    ctx.timerService().registerEventTimeTimer(ts);
                                    timerT.update(ts);
                                }

                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);

                                Iterable<Map.Entry<String, Long>> entries = avgPagePv.entries();

                                String key = ctx.getCurrentKey();


                                entries.forEach(elem -> out.collect("key = " + key + "\turl：" + elem.getKey() + "\tpv：" + elem.getValue()));


                                timerT.clear();


                            }
                        })
                .print();


        env.execute();


    }

}
