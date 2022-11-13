package com.at.conntctors.clickhouse.ck;

import org.apache.flink.util.function.BiConsumerWithException;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zero
 * @create 2022-11-13
 */
public interface ClickHouseStatementBuilder<T>
        extends BiConsumerWithException<PreparedStatement, T, SQLException>, Serializable {
}
