package com.at.conntctors.clickhouse.ckjdbc;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.function.Function;

/**
 * @create 2023-12-09
 */
public class JdbcSink {

    public static <T> SinkFunction<T> sink(
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            JdbcExecutionOptions executionOptions,
            JdbcConnectionOptions connectionOptions) {
        return new GenericJdbcSinkFunction<>(
                new JdbcOutputFormat<>(
                        new SimpleJdbcConnectionProvider(connectionOptions),
                        executionOptions,
                        context ->
                                JdbcBatchStatementExecutor.simple(
                                        sql, statementBuilder, Function.identity()),
                        JdbcOutputFormat.RecordExtractor.identity()));
    }

}
