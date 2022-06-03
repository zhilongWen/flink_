package com.at.jdbc;

import com.at.util.EnvironmentUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-04
 */
public class MysqlJDBCConnectionSQL {

    public static void main(String[] args) throws Exception{

        // --default.parallelism 1 --enable.checkpoint false --enable.table.env true

        EnvironmentUtil.Environment environment = EnvironmentUtil.getStreamExecutionEnvironment(args);

        StreamExecutionEnvironment env = environment.getEnv();
        StreamTableEnvironment tableEnv = environment.getTableEnv();


        String sourceSQL = "CREATE TABLE mysql_source_tbl(\n"
                + "    user_id int,\n"
                + "    item_id bigint,\n"
                + "    category_id int,\n"
                + "    behavior STRING,\n"
                + "    ts BIGINT\n"
                + ") WITH (\n"
                + "    'connector' = 'jdbc',\n"
                + "    'driver' = 'com.mysql.cj.jdbc.Driver',\n"
                + "    'url' = 'jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false',\n"
                + "    'table-name' = 'userbehavior_tbl',\n"
                + "    'username' = 'root',\n"
                + "    'password' = 'root'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select * from mysql_source_tbl").print();



        env.execute();


    }

}
