package com.at.sideoutputstream;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhilong.wen
 */
public class SideOutputDemo {

    // 通过 id  判断是否是同一个侧输出流，id 相同则 yes，全局唯一，只会创建一次
    //OutputTag<Event> maryOutputTag = new OutputTag<Event>("Mary-event-tag") {
    //};
    //OutputTag<Event> cartOutputTag = new OutputTag<Event>("cart-event-tag") {
    //};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                                    public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                                        maxWatermark = Math.max(maxWatermark, event.timestamp);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                        watermarkOutput.emitWatermark(new Watermark(maxWatermark - delay - 1L));
                                    }
                                };
                            }

                            @Override
                            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new TimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                };
                            }
                        }
                );

        SingleOutputStreamOperator<Event> filterProcessStream = withWatermarkStream
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {

                        OutputTag<Event> maryOutputTag = new OutputTag<Event>("Mary-event-tag") {
                        };
                        OutputTag<Event> cartOutputTag = new OutputTag<Event>("Cart-event-tag") {
                        };

                        if ("Mary".equals(value.user)) {
                            ctx.output(maryOutputTag, value);
                        }

                        if ("./cart".equals(value.url)) {
                            ctx.output(cartOutputTag, value);
                        }

                        if ("./fav".equals(value.url)) {
                            out.collect(value);
                        }

                    }
                });

        DataStream<Event> marySideOutputStream = filterProcessStream
                .getSideOutput(new OutputTag<Event>("Mary-event-tag") {
                });

        DataStream<Event> cartOutputStream = filterProcessStream
                .getSideOutput(new OutputTag<Event>("Cart-event-tag") {
                });

        marySideOutputStream.print("marySideOutputStream >>> ");
        cartOutputStream.print("cartOutputStream >>> ");
        filterProcessStream.print("filterProcessStream >>> ");

        env.execute();

    }


}
