package com.at.statebackend;

import com.at.source.ClickSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-21
 */
public class FsStateBackend {

    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 设置状态后端
        env.setStateBackend(new org.apache.flink.runtime.state.filesystem.FsStateBackend("file:///D:\\workspace\\flink_\\flink-api\\ck"));
        // 每隔 10min 做一次 checkpoint 模式为 AT_LEAST_ONCE
        env.enableCheckpointing(10 * 60 * 1000L, CheckpointingMode.AT_LEAST_ONCE);


        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置 checkpoint 最小间隔周期 1min
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
        // 设置 checkpoint 必须在 1min 内完成，否则会被丢弃
        checkpointConfig.setCheckpointTimeout(60 * 1000L);
        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
        checkpointConfig.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        // 设置 checkpoint 的并发度为 1
        checkpointConfig.setMaxConcurrentCheckpoints(1);




        env
                .addSource(new ClickSource())
                .print();

        env.execute();


    }

}
