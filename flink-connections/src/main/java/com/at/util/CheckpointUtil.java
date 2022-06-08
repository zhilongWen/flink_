package com.at.util;

import com.at.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Optional;

/**
 * @create 2022-06-04
 */
public class CheckpointUtil {

    public static StreamExecutionEnvironment enableCheckpoint(StreamExecutionEnvironment env, ParameterTool parameterTool) {

        // --enable.checkpoint true --checkpoint.type fs --checkpoint.dir hdfs://hadoop102:8020/user/flink/checkpoint/ --checkpoint.interval 1000

        // 设置状态后端
        String type = parameterTool.get(PropertiesConstants.CHECKPOINT_TYPE);

        if (PropertiesConstants.MEMORY.equals(type)) {
            MemoryStateBackend memoryStateBackend = new MemoryStateBackend(5 * 1024 * 1024);
            env.setStateBackend(memoryStateBackend);
        }

        Optional.ofNullable(parameterTool.get(PropertiesConstants.CHECKPOINT_DIR)).ifPresent(dir -> {

            System.out.println("checkpoint dir：" + dir);

            if (PropertiesConstants.FS.equals(type)) {
                FsStateBackend fsStateBackend = new FsStateBackend(dir);
                env.setStateBackend(fsStateBackend);
            } else if (PropertiesConstants.ROCKSDB.equals(type)) {
                RocksDBStateBackend rocksDBStateBackend = null;
                try {
                    rocksDBStateBackend = new RocksDBStateBackend(dir);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                env.setStateBackend(rocksDBStateBackend);
            }

        });

        // 每隔 10min 做一次 checkpoint 模式为 EXACTLY_ONCE
        env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.CHECKPOINT_INTERVAL), CheckpointingMode.EXACTLY_ONCE);

        // 设置 checkpoint 最小间隔周期 500ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置 checkpoint 必须在 1min 内完成，否则会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        // 设置 checkpoint 的并发度为 1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //手动cancel时是否保留checkpoint
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // enables the unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.configure(config);

        return env;
        
    }

}
