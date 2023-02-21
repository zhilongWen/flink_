package com.at.watermark;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @create 2022-05-15
 */
public class AssignAscendingWatermark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        // 每隔1分钟插入一次水位线  默认200ms
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);


        env
                .addSource(new ClickSource())
                // 分配升序时间戳
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps()
                )
                .print();


        

        env.execute();


    }


}
