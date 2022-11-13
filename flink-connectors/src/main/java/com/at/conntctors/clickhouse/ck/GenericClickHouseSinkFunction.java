package com.at.conntctors.clickhouse.ck;

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
import java.io.IOException;
import java.io.Serializable;

/**
 * @author zero
 * @create 2022-11-13
 */
public class GenericClickHouseSinkFunction<T> extends RichSinkFunction<T>
        implements CheckpointedFunction, InputTypeConfigurable , Serializable {

    private final ClickHouseOutputFormat<T, ?, ?> outputFormat;

    public GenericClickHouseSinkFunction(@Nonnull ClickHouseOutputFormat<T, ?, ?> outputFormat) {
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
    public void invoke(T value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void close() throws IOException {
        outputFormat.close();
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        outputFormat.setInputType(type, executionConfig);
    }
}
