package com.at.conntctors.clickhouse.ckjdbc;

import org.apache.flink.util.function.BiConsumerWithException;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @create 2023-12-09
 */
public interface JdbcStatementBuilder<T>
        extends BiConsumerWithException<PreparedStatement, T, SQLException>, Serializable {
}
