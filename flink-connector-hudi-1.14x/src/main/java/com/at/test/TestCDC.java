package com.at.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-16
 */
public class TestCDC {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("\n"
                + "create table if not exists source_order_tbl\n"
                + "(\n"
                + "    db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "    table_name STRING METADATA FROM 'table_name' VIRTUAL,\n"
                + "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "    order_id     INT,\n"
                + "    order_date   TIMESTAMP(3),\n"
                + "    customer_name string,\n"
                + "    price        double,\n"
                + "    product_id   INT,\n"
                + "    order_status BOOLEAN,\n"
                + "    ts           TIMESTAMP(3),\n"
                + "    primary key (order_id) NOT ENFORCED\n"
                + ")\n"
                + "with (\n"
                + "    'connector' = 'mysql-cdc',\n"
                + "    'hostname' = 'hadoop102',\n"
                + "    'port' = '3306',\n"
                + "    'username' = 'root',\n"
                + "    'password' = 'root',\n"
                + "    'connect.timeout' = '30s',\n"
                + "    'connect.max-retries' = '3',\n"
                + "    'connection.pool.size' = '5',\n"
                + "    'jdbc.properties.useSSL' = 'false',\n"
                + "    'jdbc.properties.characterEncoding' = 'utf-8',\n"
                + "    'database-name' = 'gmall_report',\n"
                + "    'table-name' = 'orders_tbl'\n"
                + ")\n"
                + "\n");

        tableEnv.executeSql("select * from source_order_tbl").print();



    }

}
