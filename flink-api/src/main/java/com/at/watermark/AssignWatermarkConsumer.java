package com.at.watermark;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class AssignWatermarkConsumer {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .filter(event -> "./home".equals(event.url))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {

                    // 自定义 watermark 生成


                    @Override
                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                // 指定 程序数据源里的时间戳是哪一个字段
                                return element.timestamp;
                            }
                        };
                    }

                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Event>() {

                            // max delay time 5s
                            private Long delayTime = 5000L;

                            // 系统观察的的最大时间戳   防止越界
                            private Long maxTs = - Long.MAX_VALUE + delayTime + 1L;

                            @Override
                            public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                                // 每来一条数据 更新最大时间戳
                                maxTs = Math.max(event.timestamp,maxTs);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // 发送 watermark
                                // 默认 200ms 调用一次
                                output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
                            }
                        };
                    }




                })
                .print();

        env.execute();

    }

}
