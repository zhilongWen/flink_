package com.at.conntctors.clickhouse.ck.provider;

import com.at.conntctors.clickhouse.ck.options.ClickHouseConnectorOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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


    private static final String CLUSTER_SHARD_SQL = "SELECT `shard_num`, `host_address`, `port` FROM system.clusters WHERE cluster = ? ORDER BY `shard_num`, `replica_num` ASC";

    private String dbName;
    private Optional<String> userName;
    private Optional<String> password;
    private List<String> shardUrls;

    private final String PREFIX = "jdbc:clickhouse://";
    private final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    public ClickHouseConnectionProvider(ClickHouseConnectorOptions connOptions) {
        this.connOptions = connOptions;
        this.dbName = connOptions.getUrl().substring(connOptions.getUrl().lastIndexOf('/'));
        userName = connOptions.getUsername();
        password = connOptions.getPassword();
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

        userName.ifPresent(user -> info.setProperty("user", user));
        password.ifPresent(password -> info.setProperty("password", password));

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

    // ======================================================================


    private List<List<String>> getClusterSharedUrls() throws SQLException, ClassNotFoundException {

        // key = shard_num，value = url
        Map<Integer, List<String>> map = new TreeMap<Integer, List<String>>();

        Connection conn = getOrEstablishConnection();

        PreparedStatement stmt = conn.prepareStatement(CLUSTER_SHARD_SQL);

        stmt.setString(1, connOptions.getCluster().get());

        ResultSet resultSet = stmt.executeQuery();

        while (resultSet.next()) {

            int shardNum = resultSet.getInt("shard_num");
            String hostAddress = resultSet.getString("host_address");
            int port = resultSet.getInt("port");

            //jdbc:clickhouse://hadoop102:8123/testdb01
            String url = PREFIX + hostAddress + ":" + port + "/" + dbName;

            map.computeIfAbsent(shardNum, k -> new ArrayList<>());
            map.get(shardNum).add(url);

        }

        return map.values().stream().collect(Collectors.toList());

    }

    public void getShardConnectionAny(List<String> shardUrls){



    }


}
