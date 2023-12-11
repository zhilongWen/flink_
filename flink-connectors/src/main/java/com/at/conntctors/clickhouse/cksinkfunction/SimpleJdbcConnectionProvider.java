package com.at.conntctors.clickhouse.cksinkfunction;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

/**
 * @create 2023-12-09
 */
public class SimpleJdbcConnectionProvider implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;

    private transient Driver loadedDriver;
    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }


    private static Driver loadDriver(String driverName) throws ClassNotFoundException, SQLException {

        Preconditions.checkNotNull(driverName);

//        Enumeration<Driver> drivers = DriverManager.getDrivers();
//        while (drivers.hasMoreElements()) {
//            Driver driver = drivers.nextElement();
//            if (driver.getClass().getName().equals(driverName)){
//                return driver;
//            }
//        }

        Class<?> clazz = Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());

        try {
            return (Driver) clazz.newInstance();
        } catch (Exception e) {
            throw new SQLException("Fail to create driver of class " + driverName, e);
        }

    }


    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean isConnectionValid() throws SQLException {
        return connection != null && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
    }

    public Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(jdbcOptions.driverName);
        }

        return loadedDriver;
    }

    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {

        if (connection != null) {
            return connection;
        }

        if (jdbcOptions.getDriverName() == null) {
            connection = DriverManager.getConnection(
                    jdbcOptions.getDbURL(),
                    jdbcOptions.getUsername().orElseGet(() -> null),
                    jdbcOptions.getPassword().orElseGet(() -> null)
            );
        } else {

            Driver driver = getLoadedDriver();

            Properties properties = new Properties();
            jdbcOptions.getUsername().ifPresent(u -> properties.setProperty("user", u));
            jdbcOptions.getPassword().ifPresent(p -> properties.setProperty("password", p));

            connection = driver.connect(jdbcOptions.getDbURL(), properties);

            if (connection == null) {
                // Throw same exception as DriverManager.getConnection when no driver found to match
                // caller expectation.
                throw new SQLException(
                        "No suitable driver found for " + jdbcOptions.getDbURL(), "08001");
            }

        }

        return connection;

    }

    public JdbcConnectionOptions getJdbcOptions() {
        return jdbcOptions;
    }

    public Connection getConnection(String url, String userName, String passwd) throws SQLException, ClassNotFoundException {

        Driver driver = getLoadedDriver();

        Properties properties = new Properties();
        Optional.ofNullable(userName).ifPresent(u -> properties.setProperty("user",u));
        Optional.ofNullable(passwd).ifPresent(p -> properties.setProperty("password",p));


        return driver.connect(url,properties );

    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }

}
