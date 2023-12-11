package com.at.conntctors.clickhouse.ckjdbc;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * @create 2023-12-10
 */
public class GenericJdbcSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction, InputTypeConfigurable {

    private final JdbcOutputFormat<T, ?, ?> outputFormat;

    public GenericJdbcSinkFunction(@Nonnull JdbcOutputFormat<T, ?, ?> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        outputFormat.setInputType(type, executionConfig);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
    }
}
