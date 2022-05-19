package com.at;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @create 2022-05-20
 */
public class LatenessData {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);




        /*
            a 1
            w 2
            r 5
            t 1
         */
        env
                .socketTextStream("127.0.0.1", 9099)
                .map(value -> Tuple2.of(value.split(" ")[0], Long.parseLong(value.split(" ")[1]) * 1000L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((Tuple2<String, Long> element, long recordTimestamp) -> element.f1))
                .process(

                        // ProcessFunction 不能使用 状态变量 不能使用 onTimer 编译会出错
                        new ProcessFunction<Tuple2<String, Long>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {

                                long watermark = ctx.timerService().currentWatermark();

                                if (value.f1 < watermark) {
                                    out.collect("watermark = " + watermark + "\t当前迟到元素：" + value.f0);
                                } else {
                                    out.collect("watermark = " + watermark + "\t当元素：" + value.f0);
                                }

                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);

                            }
                        })
                .print();


        env.execute();


    }

}
