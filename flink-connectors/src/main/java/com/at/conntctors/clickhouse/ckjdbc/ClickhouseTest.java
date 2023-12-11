package com.at.conntctors.clickhouse.ckjdbc;

import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @create 2023-12-10
 */
public class ClickhouseTest {



    static ClickHouseConnection connection;

    static {

        try {

            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

            connection  = (ClickHouseConnection) DriverManager.getConnection("jdbc:clickhouse://hadoop104:8123/testdb01");

        }catch (Exception e){


        }

    }

    public static void main(String[] args) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        String insertSQL = "insert into st_order_mt_db01 values(?,?,?,?)";

        PreparedStatement stem = connection.prepareStatement(insertSQL);

        stem.setInt(1,100);
        stem.setString(2,"3609");
        stem.setDouble(3,78.0D);
        stem.setString(4,sdf.format(new Date()));

        stem.execute();
        connection.commit();

    }
}
