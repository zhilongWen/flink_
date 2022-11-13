package com.at.conntctors.clickhouse.ck;

import com.at.conntctors.clickhouse.ck.executor.ClickHouseBatchStatementExecutor;
import com.at.conntctors.clickhouse.ck.options.ClickHouseExecutionOptions;
import com.at.conntctors.clickhouse.ck.provider.ClickHouseConnectionProvider;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author zero
 * @create 2022-11-13
 */
public class ClickHouseOutputFormat<IN, JdbcIn, JdbcExec extends ClickHouseBatchStatementExecutor<JdbcIn>>
        extends RichOutputFormat<IN> implements Flushable, InputTypeConfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseOutputFormat.class);

    private static final long serialVersionUID = 1L;

    protected final ClickHouseConnectionProvider connectionProvider;

    @Nullable
    private TypeSerializer<IN> serializer;

    private final ClickHouseExecutionOptions executionOptions;
    private final StatementExecutorFactory<JdbcExec> statementExecutorFactory;
    private final RecordExtractor<IN, JdbcIn> recordExtractor;

    private transient JdbcExec statementExecutor;
    private transient int batchSize = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;


    public interface StatementExecutorFactory<T extends ClickHouseBatchStatementExecutor<?>>
            extends SerializableFunction<RuntimeContext, T> {
    }

    public interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
        static <T> ClickHouseOutputFormat.RecordExtractor<T, T> identity() {
            return x -> x;
        }
    }


    public ClickHouseOutputFormat(
            @Nonnull ClickHouseConnectionProvider connectionProvider,
            @Nonnull ClickHouseExecutionOptions executionOptions,
            @Nonnull ClickHouseOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory,
            @Nonnull ClickHouseOutputFormat.RecordExtractor<IN, JdbcIn> recordExtractor) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executionOptions = Preconditions.checkNotNull(executionOptions);
        this.statementExecutorFactory = Preconditions.checkNotNull(statementExecutorFactory);
        this.recordExtractor = Preconditions.checkNotNull(recordExtractor);
    }


    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (executionConfig.isObjectReuseEnabled()) {
            this.serializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        // TODO 获取 ClickHouse 连接，启动定时任务
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new IOException("unable to open ClickHouse writer", e);
        }
        // TODO 获取 statementExecutor
//        DistributeBatchStatementExecutor<IN, Recycler.V> statementExecutor = new DistributeBatchStatementExecutor<>(null, null);

//        statementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);

        statementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);

        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {

            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("ClickHouse-jdbc-upsert-output-format"));

            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
                    () -> {
                        synchronized (ClickHouseOutputFormat.this) {
                            if (!closed) {
                                try {
                                    flush();
                                } catch (Exception e) {
                                    flushException = e;
                                }
                            }
                        }
                    },
                    executionOptions.getBatchIntervalMs(),
                    executionOptions.getBatchIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        }
    }


    private JdbcExec createAndOpenStatementExecutor(
            ClickHouseOutputFormat.StatementExecutorFactory<JdbcExec> statementExecutorFactory) throws IOException {

        JdbcExec exec = statementExecutorFactory.apply(getRuntimeContext());

        try {
            exec.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            throw new IOException("unable to open JDBC writer", e);
        }
        return exec;
    }


    @Override
    public void flush() throws IOException {
        // TODO 将数据写入 stm
        checkFlushException();

        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchSize = 0;
                break;
            } catch (SQLException e) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                } catch (Exception exception) {
                    LOG.error(
                            "ClickHouse connection is not valid, and reestablish connection failed.",
                            exception);
                    throw new IOException("Reestablish ClickHouse connection failed", exception);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }


    protected void attemptFlush() throws SQLException {
        statementExecutor.executeBatch();
    }

    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        statementExecutor.closeStatements();
        statementExecutor.prepareStatements(
                reconnect
                        ? connectionProvider.reestablishConnection()
                        : connectionProvider.getConnection());
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to ClickHouse failed.", flushException);
        }
    }

    @Override
    public void configure(Configuration parameters) {

    }


    @Override
    public final synchronized void writeRecord(IN record) throws IOException {

        checkFlushException();

        try {

            IN recordCopy = copyIfNecessary(record);
            addToBatch(recordCopy, recordExtractor.apply(recordCopy));
            batchSize++;

            if (executionOptions.getBatchSize() > 0 && batchSize >= executionOptions.getBatchSize()) {
                flush();
            }


        } catch (Exception e) {
            throw new IOException("Writing records to ClickHouse failed.", e);
        }

    }

    protected void addToBatch(IN original, JdbcIn extracted) throws SQLException {
        statementExecutor.addToBatch(extracted);
    }

    private IN copyIfNecessary(IN record) {
        return serializer == null ? record : serializer.copy(record);
    }

    @Override
    public void close() throws IOException {

        if (!closed) {

            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchSize > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to ClickHouse failed.", e);
                    throw new RuntimeException("Writing records to ClickHouse failed.", e);
                }
            }

            try {
                if (statementExecutor != null) {
                    statementExecutor.closeStatements();
                }
            } catch (SQLException e) {
                LOG.warn("Close ClickHouse writer failed.", e);
            }

            connectionProvider.closeConnection();
            checkFlushException();

        }

    }


}
