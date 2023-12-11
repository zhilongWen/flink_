package com.at.conntctors.clickhouse.cksinkfunction;

import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @create 2023-12-10
 */
public class ClickHouseUtils {

    public static Map<Integer, List<String>> getShareUrls(JdbcConnectionOptions options, Connection conn) throws Exception {

        String sql = "select engine_full \n"
                + "from system.tables\n"
                + "where database = '" + options.getDatabase() + "' \n"
                + "and name = 'st_order_mt_db01_all'";


        PreparedStatement prepareStatement = conn.prepareStatement(sql);

        String clusterName = "";
        ResultSet resultSet = prepareStatement.executeQuery();

        if (resultSet.next()) {
            String engine_full = resultSet.getString("engine_full");
            clusterName = Arrays.stream(engine_full.substring(12).split(","))
                    .map(s -> s.replace("'", "").trim())
                    .collect(Collectors.toList())
                    .get(0);
        }

        sql = "SELECT \n"
                + "    shard_num, \n"
                + "    host_address, \n"
                + "    port ,\n"
                + "    shard_weight\n"
                + "FROM system.clusters WHERE cluster = '" + clusterName + "' \n"
                + "ORDER BY shard_num, replica_num ASC";

        prepareStatement = conn.prepareStatement(sql);

        resultSet = prepareStatement.executeQuery();

        Map<Integer, List<String>> shardsMap = new HashMap<>();

        while (resultSet.next()) {

            int shard_num = resultSet.getInt("shard_num");
            String host_address = resultSet.getString("host_address");
            int port = resultSet.getInt("port");

            List<String> urls = shardsMap.computeIfAbsent(shard_num, k -> new ArrayList<>());
            urls.add("jdbc:clickhouse://" + host_address + ":" + options.getPort() + "/" + options.database);

        }

        return shardsMap;


    }

    public static void main(String[] args) throws Exception{

//        System.out.println(System.currentTimeMillis());
//
//        String url = "jdbc:clickhouse://hadoop102:8123/testdb01";
//        String port = url.substring(url.indexOf("//") + 2, url.lastIndexOf("/")).split(":")[1];
//        String database = url.substring(url.lastIndexOf("/") + 1);
//
//        System.out.println(port);
//        System.out.println(database);

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

        ClickHouseConnection connection = (ClickHouseConnection) DriverManager.getConnection("jdbc:clickhouse://hadoop102:8123/testdb01");


        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.Builder()
                .withUrl("jdbc:clickhouse://hadoop102:8123/testdb01")
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .build();

        Map<Integer, List<String>> shareUrls = getShareUrls(jdbcConnectionOptions, connection);

        System.out.println(shareUrls);


    }

}
