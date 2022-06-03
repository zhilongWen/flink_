package com.at.util;

import com.at.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Optional;

/**
 * @create 2022-06-04
 */
public class CheckpointUtil {

    public static void enableCheckpoint(StreamExecutionEnvironment env, ParameterTool parameterTool) {


        // 设置状态后端
        String type = parameterTool.get(PropertiesConstants.CHECKPOINT_TYPE);

        if (PropertiesConstants.MEMORY.equals(type)) {
            MemoryStateBackend memoryStateBackend = new MemoryStateBackend(5 * 1024 * 1024);
            env.setStateBackend(memoryStateBackend);
        }

        Optional.ofNullable(parameterTool.get(PropertiesConstants.CHECKPOINT_DIR)).ifPresent(dir -> {

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

        // 每隔 10min 做一次 checkpoint 模式为 AT_LEAST_ONCE
        env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.CHECKPOINT_INTERVAL), CheckpointingMode.AT_LEAST_ONCE);

        // 设置 checkpoint 最小间隔周期 500ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置 checkpoint 必须在 1min 内完成，否则会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        // 设置 checkpoint 的并发度为 1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    }

}
