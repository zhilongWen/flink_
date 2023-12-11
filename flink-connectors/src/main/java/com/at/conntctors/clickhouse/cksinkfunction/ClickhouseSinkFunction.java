package com.at.conntctors.clickhouse.cksinkfunction;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @create 2023-12-11
 */
public class ClickhouseSinkFunction extends RichSinkFunction<Tuple4<Integer, String, Double, String>> implements CheckpointedFunction {

    Map<Integer, Connection> shardMap;
    Map<Integer, AtomicInteger> counterMap;
    Map<Integer, PreparedStatement> statementMap;

    Map<Integer, List<Tuple4<Integer, String, Double, String>>> values;
    Random random = new Random();


    protected transient ScheduledExecutorService scheduler;
    protected transient ScheduledFuture<?> scheduledFuture;

    protected transient volatile boolean closed = false;
    protected transient volatile Exception flushException;

    private SimpleJdbcConnectionProvider connectionProvider;
    String sql;

    public ClickhouseSinkFunction(String sql, SimpleJdbcConnectionProvider connectionProvider) {
        this.sql = sql;
        this.connectionProvider = connectionProvider;
        values = new HashMap<>();
        counterMap = new HashMap<>();
        statementMap = new HashMap<>();
        shardMap = new HashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {


        Map<Integer, List<String>> shareUrls = ClickHouseUtils.getShareUrls(connectionProvider.getJdbcOptions(), connectionProvider.getOrEstablishConnection());

        List<String> urls = shareUrls.values().stream().flatMap(r -> r.stream()).collect(Collectors.toList());

        int len = urls.size();
        int[] shardIds = new int[len];
        Map<Integer, Connection> shardMap = new HashMap<>(len);

        for (int i = 0; i < urls.size(); i++) {
            shardIds[i] = i;
            Connection conn = getConnection(urls.get(i));
            shardMap.put(i, conn);

            statementMap.put(i, conn.prepareStatement(sql));

        }


        scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("sink-ck"));
        scheduledFuture =
                scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        10000,
                        10000,
                        TimeUnit.MILLISECONDS);
    }

    public Connection getConnection(String url) throws SQLException, ClassNotFoundException {
        return connectionProvider.getConnection(url,null,null);
    }

    @Override
    public void invoke(Tuple4<Integer, String, Double, String> value, Context context) throws Exception {

        int index = (value.hashCode() & Integer.MAX_VALUE) % statementMap.size();
        List<Tuple4<Integer, String, Double, String>> tuple4s = values.computeIfAbsent(index, r -> new ArrayList<>());
        tuple4s.add(value);
//        values.put(index, tuple4s);

        int batchCount =
                counterMap
                        .computeIfAbsent(index, integer -> new AtomicInteger(0))
                        .incrementAndGet();

        if (batchCount > 10000) {
            flush(index);
        }

    }

    private synchronized void flush(int index) throws SQLException {

        List<Tuple4<Integer, String, Double, String>> batchData = values.get(index);
        PreparedStatement statement = statementMap.get(index);

        if (!batchData.isEmpty()) {
            for (Tuple4<Integer, String, Double, String> data : batchData) {

                statement.setInt(1, data.f0);
                statement.setString(2, data.f1);
                statement.setDouble(3, data.f2);
                statement.setString(4, data.f3);

                statement.addBatch();

            }
        }

        statement.executeBatch();

        counterMap.get(index).set(0);
        values.get(index).clear();


    }

    public void flush() throws SQLException {



        for (Map.Entry<Integer, List<Tuple4<Integer, String, Double, String>>> entry : values.entrySet()) {

            if (!entry.getValue().isEmpty()) {
                flush(entry.getKey());
            }

        }

    }

    @Override
    public void finish() throws Exception {
        flush();
        closed = !closed;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        flush();
    }
}
