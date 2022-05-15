package com.at.watermark;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class AssignWatermarkConsumerForPunctuated {

    public static void main(String[] args) throws Exception {

        // 自定义 非周期性 watermark

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Event>() {

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
                                return new WatermarkGenerator<Event>() {
                                    @Override
                                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                                        // Alice click event emit watermark
                                        if ("Alice".equals(event.user)) {
                                            output.emitWatermark(new Watermark(event.timestamp - 1L));
                                        }
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 非周期性发送 watermark  该方法无需任何操作
                                    }
                                };
                            }
                        }
                )
                .print();


        env.execute();


    }

}
