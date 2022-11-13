package com.at.conntctors.clickhouse.ck.executor;

import com.at.conntctors.clickhouse.ck.ClickHouseStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author zero
 * @create 2022-11-13
 */
public class DistributeBatchStatementExecutor<T, V> implements ClickHouseBatchStatementExecutor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DistributeBatchStatementExecutor.class);

    private String sql;

    private PreparedStatement statement;

    private final Function<T, V> valueTransformer;

    private List<V> batch;

    private ClickHouseStatementBuilder<V> parameterSetter;


    public DistributeBatchStatementExecutor(
            String sql,
            ClickHouseStatementBuilder<V> statementBuilder,
            Function<T, V> valueTransformer) {
        this.sql = sql;
        this.batch = new ArrayList<>();
        this.parameterSetter = statementBuilder;
        this.valueTransformer = valueTransformer;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statement = connection.prepareStatement(this.sql);
    }

    @Override
    public void addToBatch(T record) throws SQLException {
        batch.add(valueTransformer.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (V v : batch) {
                parameterSetter.accept(statement, v);
                statement.addBatch();
            }
            statement.executeBatch();
            batch.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
    }
}
