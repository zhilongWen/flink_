package com.at.watermark;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.EventListener;

/**
 * @create 2022-05-15
 */
public class AssignWatermark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 每隔1分钟插入一次水位线  默认200ms
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);


        env
                .addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // forBoundedOutOfOrderness 针对乱序流插入水位线
                        // 最大延迟时间设置为5s
                        // 默认200ms的机器时间插入一个水位线
                        // 水位线 = 观察到的元素中的最大时间戳 - 最大延迟时间 - 1ms
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            // 抽取时间戳的逻辑
                                            // 为什么要抽取时间戳？  如果不指定元素中时间戳的字段，程序就不知道事件时间是哪个字段
                                            // 时间戳的单位必须是毫秒
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                )
                .print();


        env.execute();

    }

}
