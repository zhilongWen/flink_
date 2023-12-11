package com.at.conntctors.clickhouse.ckjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @create 2023-12-09
 */
public class SimpleBatchStatementExecutor<T, V> implements JdbcBatchStatementExecutor<T> {


    private static final Logger LOG = LoggerFactory.getLogger(SimpleBatchStatementExecutor.class);

    private final String sql;
    private final JdbcStatementBuilder<V> parameterSetter;
    private final Function<T, V> valueTransformer;
    private final List<V> batch;

    private transient PreparedStatement st;

    SimpleBatchStatementExecutor(
            String sql, JdbcStatementBuilder<V> statementBuilder, Function<T, V> valueTransformer) {
        this.sql = sql;
        this.parameterSetter = statementBuilder;
        this.valueTransformer = valueTransformer;
        this.batch = new ArrayList<>();
    }


    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.st = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(T record) throws SQLException {
        batch.add(valueTransformer.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {

        if (!batch.isEmpty()) {
            for (V v : batch) {
                parameterSetter.accept(st, v);
                st.addBatch();
            }

            st.executeBatch();
            batch.clear();
        }

    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }
}
