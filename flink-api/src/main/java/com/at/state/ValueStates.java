package com.at.state;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @create 2022-05-20
 */
public class ValueStates {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((Event element, long recordTimestamp) -> element.timestamp)
                )
                .keyBy(event -> event.user)
                .process(

                        // 每隔 10s 统计一下 pv
                        new KeyedProcessFunction<String, Event, String>() {

                            private ValueState<Long> pvState;
                            private ValueState<Long> timerTs;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                pvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Types.LONG));
                                timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Types.LONG));
                            }

                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                                if (pvState.value() == null) {
                                    pvState.update(1L);
                                } else {
                                    pvState.update(pvState.value() + 1);
                                }

                                if (timerTs.value() == null) {
                                    timerTs.update(value.timestamp + 5 * 1000L);
                                    ctx.timerService().registerEventTimeTimer(value.timestamp + 5 * 1000L);
                                }

                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                out.collect("key = " + ctx.getCurrentKey() + " 5s pv = " + pvState.value());
                                timerTs.clear();
                            }
                        })
                .print();


        env.execute();


    }

}
