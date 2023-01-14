package com.at.conntctors.clickhouse.ck.executor;


import com.at.conntctors.clickhouse.ck.ClickHouseStatementBuilder;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * @author zero
 * @create 2022-11-13
 */
public interface ClickHouseBatchStatementExecutor<T> extends Serializable {


    /**
     * Create statements from connection.
     */
    void prepareStatements(Connection connection) throws SQLException;

    void addToBatch(T record) throws SQLException;

    /**
     * Submits a batch of commands to the database for execution.
     */
    void executeBatch() throws SQLException;

    /**
     * Close JDBC related statements.
     */
    void closeStatements() throws SQLException;


    static <T, V> ClickHouseBatchStatementExecutor<T> simple(
            String sql, ClickHouseStatementBuilder<V> paramSetter, Function<T, V> valueTransformer) {
        return new DistributeBatchStatementExecutor<T, V>(sql, paramSetter, valueTransformer);
    }

}
