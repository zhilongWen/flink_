package com.at.sideoutputstream;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @create 2022-05-20
 */
public class SideOutputStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        OutputTag<Event> maryOutputStream = new OutputTag<Event>("maryOutputStream", TypeInformation.of(Event.class));

        // filter flatMap Output

        SingleOutputStreamOperator<Event> streamOperator = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                )
                .process(
                        new ProcessFunction<Event, Event>() {
                            @Override
                            public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {

                                if ("Mary".equals(event.user)) {
                                    ctx.output(maryOutputStream, event);
                                } else {
                                    out.collect(event);
                                }

                            }
                        }
                );

        streamOperator.getSideOutput(maryOutputStream).print();

        env.execute();


    }

}
