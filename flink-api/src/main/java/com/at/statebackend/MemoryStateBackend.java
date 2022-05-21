package com.at.statebackend;

import com.at.source.ClickSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-21
 */
public class MemoryStateBackend {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // state 存放在内存中，默认是 5M
        checkpointConfig.setCheckpointStorage(new org.apache.flink.runtime.state.memory.MemoryStateBackend(10 * 1024 * 1024));
        // 设置模式为精确一次 默认值
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        checkpointConfig.setCheckpointTimeout(60000);
        // 允许两个连续的 checkpoint 错误
        checkpointConfig.setTolerableCheckpointFailureNumber(2);
        // 同一时间只允许一个 checkpoint 进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 开启实验性的 unaligned checkpoints
        checkpointConfig.enableUnalignedCheckpoints(true);


        env
                .addSource(new ClickSource())
                .print();

        env.execute();






    }

}
