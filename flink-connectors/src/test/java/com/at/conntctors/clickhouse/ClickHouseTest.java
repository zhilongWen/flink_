package com.at.conntctors.clickhouse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @create 2022-11-15
 */
public class ClickHouseTest {

    private final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    public final int CONNECTION_CHECK_TIMEOUT_SECONDS = 60;
    public final String URL = "jdbc:clickhouse://hadoop102:8123/testdb01";

    static ClickHouseConnection connection;


    @Before
    public void setUp() {

        try {
            Class.forName(CLICKHOUSE_DRIVER_NAME);

            connection = (ClickHouseConnection) DriverManager.getConnection(URL);

            connection.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS);

        } catch (Exception e) {

        }

    }

    @After
    public void tearDown() {

    }

    @Test
    public void test1() throws Exception {

        String sql = "SELECT `shard_num`, `host_address`, `port` FROM system.clusters WHERE cluster = ? ORDER BY `shard_num`, `replica_num` ASC";

        PreparedStatement stem = connection.prepareStatement(sql);

        stem.setString(1, "gmall_cluster");

        ResultSet resultSet = stem.executeQuery();

        Map<Integer, List<String>> map = new TreeMap<Integer, List<String>>();

        String dbName = URL.substring(URL.lastIndexOf('/'));

        while (resultSet.next()) {

            int shardNum = resultSet.getInt("shard_num");
            String hostAddress = resultSet.getString("host_address");
            int port = resultSet.getInt("port");

            //jdbc:clickhouse://hadoop102:8123/testdb01

            String url = "jdbc:clickhouse://" + hostAddress + ":" + port + "/" + dbName;

            map.computeIfAbsent(shardNum, k -> new ArrayList<>());
            map.get(shardNum).add(url);

        }

        map.forEach((k,v) -> System.out.println(k + " -> " + v));

        List<List<String>> collect = map.values().stream().collect(Collectors.toList());

        System.out.println(collect);

    }

}
