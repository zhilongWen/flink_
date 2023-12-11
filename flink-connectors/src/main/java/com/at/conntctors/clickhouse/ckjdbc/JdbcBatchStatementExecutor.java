package com.at.conntctors.clickhouse.ckjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * @create 2023-12-09
 */
public interface JdbcBatchStatementExecutor<T> {


    /** Create statements from connection. */
    void prepareStatements(Connection connection) throws SQLException;

    void addToBatch(T record) throws SQLException;

    /** Submits a batch of commands to the database for execution. */
    void executeBatch() throws SQLException;

    /** Close JDBC related statements. */
    void closeStatements() throws SQLException;


    static <T, V> JdbcBatchStatementExecutor<T> simple(
            String sql, JdbcStatementBuilder<V> paramSetter, Function<T, V> valueTransformer) {
        return new SimpleBatchStatementExecutor<>(sql, paramSetter, valueTransformer);
    }
}
