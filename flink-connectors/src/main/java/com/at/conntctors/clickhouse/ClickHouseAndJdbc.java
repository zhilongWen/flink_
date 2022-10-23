package com.at.conntctors.clickhouse;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zero
 * @create 2022-10-23
 */
public class ClickHouseAndJdbc {

    /*

    <!-- clickhouse -->
		<dependency>
			<groupId>ru.yandex.clickhouse</groupId>
			<artifactId>clickhouse-jdbc</artifactId>
			<version>0.2.4</version>
			<exclusions>
				<exclusion>
					<groupId>com.fasterxml.jackson.core</groupId>
					<artifactId>jackson-databind</artifactId>
				</exclusion>
				<exclusion>
					<groupId>com.fasterxml.jackson.core</groupId>
					<artifactId>jackson-core</artifactId>
				</exclusion>
			</exclusions>
		</dependency>


		zk.sh start

		systemctl start clickhouse-server

		clickhouse-client -m

		create database testdb01;

		use testdb01;
		# 创建分布式表
        create table st_order_mt_db01_all on cluster gmall_cluster
        (
            id UInt32,
            sku_id String,
            total_amount Decimal(16,2),
            create_time  Datetime
        )engine = Distributed(gmall_cluster,testdb01, st_order_mt_db01,hiveHash(sku_id))
        ;

        # 创建本地表
        create table st_order_mt_db01 on cluster gmall_cluster
        (
            id UInt32,
            sku_id String,
            total_amount Decimal(16,2),
            create_time  Datetime
        ) engine =ReplicatedMergeTree('/clickhouse/tables/{shard}/st_order_mt_db01','{replica}')
        partition by toYYYYMMDD(create_time)
        primary key (id)
        order by (id,sku_id)
        ;

        insert into st_order_mt_db01 values(?,?,?,?)





     */

    private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final int CONNECTION_CHECK_TIMEOUT_SECONDS = 60;
    public static final String URL = "jdbc:clickhouse://hadoop102:8123/testdb01";

    static ClickHouseConnection connection;

    static {

        try {
            Class.forName(CLICKHOUSE_DRIVER_NAME);

            connection = (ClickHouseConnection) DriverManager.getConnection(URL);

            connection.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS);

        } catch (Exception e) {

        }
    }

    public static void main(String[] args) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        String insertSQL = "insert into st_order_mt_db01 values(?,?,?,?)";

        PreparedStatement stem = connection.prepareStatement(insertSQL);

        stem.setInt(1,219);
        stem.setString(2,"3609");
        stem.setDouble(3,78.0D);
        stem.setString(4,sdf.format(new Date()));

        stem.execute();
        connection.commit();
    }


}
