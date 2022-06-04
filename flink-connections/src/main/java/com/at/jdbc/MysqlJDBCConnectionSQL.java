package com.at.jdbc;

import com.at.util.EnvironmentUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-04
 */
public class MysqlJDBCConnectionSQL {

    public static void main(String[] args) throws Exception{

        // --default.parallelism 1 --enable.checkpoint true --checkpoint.type fs --checkpoint.dir file:///D:\\workspace\\flink_\\files\\ck --checkpoint.interval 60000 --enable.table.env true

        // --execute.mode batch --enable.table.env true

//        EnvironmentUtil.Environment environment = EnvironmentUtil.getStreamExecutionEnvironment(args);
//
//        StreamExecutionEnvironment env = environment.getEnv();
//        StreamTableEnvironment tableEnv = environment.getTableEnv();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


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
                + "    'password' = 'root',\n"
                + "    'connection.max-retry-timeout' = '60S', \n"
                + "    'lookup.cache.max-rows' = '100',\n"
                + "    'lookup.cache.ttl' = '60',\n"
                + "    'scan.partition.column' = 'user_id',\n"
                + "    'scan.partition.num' = '1000',\n"
                + "    'scan.partition.lower-bound' = '0',\n"
                + "    'scan.partition.upper-bound' = '1000000000'\n"
                + ")";

        String dimSourceSQL = "CREATE TABLE mysql_source_rule_tbl(\n"
                + "    user_id int,\n"
                + "    name string,\n"
                + "    age int,\n"
                + "    sex int,\n"
                + "    address string\n"
                + ") WITH (\n"
                + "    'connector' = 'jdbc',\n"
                + "    'driver' = 'com.mysql.cj.jdbc.Driver',\n"
                + "    'url' = 'jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false',\n"
                + "    'table-name' = 'rule_table',\n"
                + "    'username' = 'root',\n"
                + "    'password' = 'root',\n"
                + "    'connection.max-retry-timeout' = '60S', \n"
                + "    'lookup.cache.max-rows' = '100',\n"
                + "    'lookup.cache.ttl' = '60'  \n"
                + ")";


        tableEnv.executeSql(sourceSQL);
        tableEnv.executeSql(dimSourceSQL);

        String sinkSQL = "CREATE TABLE IF NOT EXISTS sin_user_tbl(\n"
                + "    user_id int,\n"
                + "    name string,\n"
                + "    age int,\n"
                + "    sex int,\n"
                + "    item_id bigint,\n"
                + "    category_id int,\n"
                + "    behavior string,\n"
                + "    ts bigint,\n"
                + "    address string,"
                + "    PRIMARY KEY(user_id) NOT ENFORCED \n" // Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode
                + ") \n"
                + "WITH (\n"
                + "    'connector' = 'jdbc',\n"
                + "    'driver' = 'com.mysql.cj.jdbc.Driver',\n"
                + "    'url' = 'jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false',\n"
                + "    'table-name' = 'user_tbl',\n"
                + "    'username' = 'root',\n"
                + "    'password' = 'root',\n"
                + "    'connection.max-retry-timeout' = '60S', \n"
                + "    'sink.buffer-flush.max-rows' = '50',\n"
                + "    'sink.buffer-flush.interval' = '1s',\n"
                + "    'sink.max-retries' = '3',\n"
                + "    'sink.parallelism' = '1'\n"
                + ")";

        String insertSQL = "INSERT INTO sin_user_tbl \n"
                + "SELECT\n"
                + "    A.user_id AS user_id,\n"
                + "    B.name AS name,\n"
                + "    B.age AS age,\n"
                + "    B.sex AS sex,\n"
                + "    A.item_id AS item_id,\n"
                + "    A.category_id AS category_id,\n"
                + "    A.behavior AS behavior,\n"
                + "    A.ts AS ts,\n"
                + "    B.address as address\n"
                + "FROM mysql_source_tbl A RIGHT JOIN mysql_source_rule_tbl B ON A.user_id=B.user_id";


        tableEnv.executeSql(sinkSQL);
        tableEnv.executeSql(insertSQL);

        tableEnv.executeSql("select * from sin_user_tbl").print();



//        env.execute();  // No operators defined in streaming topology. Cannot execute.


    }

}
