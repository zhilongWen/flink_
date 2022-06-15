package com.at.cdc;

import com.at.util.EnvironmentUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-08
 */
public class MySQLCDCConnectorSQL {

    public static void main(String[] args) {

        // --execute.mode stream --enable.table.env true --enable.checkpoint true --checkpoint.interval 3000 --checkpoint.type fs --checkpoint.dir file:///D:\\workspace\\flink_\\files\\ck -Xmx 100m -Xms 100m

        EnvironmentUtil.Environment environment = EnvironmentUtil.getExecutionEnvironment(args);

        StreamExecutionEnvironment env = environment.getEnv();
        StreamTableEnvironment tableEnv = environment.getTableEnv();

        String sourceSQL = "create table if not exists source_order_tbl\n"
                + "(\n"
                + "    db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "    table_name STRING METADATA  FROM 'table_name' VIRTUAL,\n"
                + "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "    order_id      int,\n"
                + "    order_date    TIMESTAMP(0),\n"
                + "    customer_name string,\n"
                + "    price         decimal(10, 5),\n"
                + "    product_id    int,\n"
                + "    order_status  boolean,\n"
                + "    primary key (order_id) NOT ENFORCED\n"
                + ") WITH (\n"
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
                + ")\n";

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select * from source_order_tbl").print();





    }

}
