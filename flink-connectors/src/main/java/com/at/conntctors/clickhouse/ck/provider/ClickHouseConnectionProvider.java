package com.at.conntctors.clickhouse.ck.provider;

import com.at.conntctors.clickhouse.ck.options.ClickHouseConnectorOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * @author zero
 * @create 2022-11-13
 */
public class ClickHouseConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private ClickHouseConnectorOptions connOptions;

    private transient Driver loadedDriver;
    private transient Connection connection;

    protected final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    public ClickHouseConnectionProvider(ClickHouseConnectorOptions connOptions) {
        this.connOptions = connOptions;
    }

    @Nullable
    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && connection.isValid(connOptions.getConnectionCheckTimeoutSeconds());
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {

        if (connection != null) {
            return connection;
        }

        Driver driver = getLoadedDriver();
        Properties info = new Properties();
        connOptions.getUsername().ifPresent(user -> info.setProperty("user", user));
        connOptions.getPassword().ifPresent(password -> info.setProperty("password", password));
        connection = driver.connect(connOptions.getUrl(), info);

        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new SQLException(
                    "No suitable driver found for " + connOptions.getUrl(), "08001");
        }

        return connection;
    }

    private Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(DRIVER_NAME);
        }
        return loadedDriver;
    }

    private static Driver loadDriver(String driverName)
            throws SQLException, ClassNotFoundException {
        Preconditions.checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.newInstance();
        } catch (Exception ex) {
            throw new SQLException("Fail to create driver of class " + driverName, ex);
        }
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("ClickHouse connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }
}
