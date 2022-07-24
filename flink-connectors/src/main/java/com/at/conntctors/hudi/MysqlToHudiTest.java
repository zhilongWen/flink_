package com.at.conntctors.hudi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-14
 */
public class MysqlToHudiTest {

    public static void main(String[] args) throws Exception {

        System.setProperty("fs.default.name","hdfs");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

//        env.enableCheckpointing(60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage(new Path("file:///D:\\workspace\\workspace2021\\bigdata-learn\\flink-learning\\flink-hudi-test\\ck"));
////        env.setStateBackend(new FsStateBackend("hdfs:///hadoop102:8020/testhudi/ck/MysqlToHudiTest"));
//        env.getCheckpointConfig().setCheckpointTimeout(2 * 60 * 1000L);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);




        String sourceCDCSQL = "CREATE TABLE mysql_users (\n" +
                "    id BIGINT PRIMARY KEY NOT ENFORCED ,\n" +
                "    name STRING,\n" +
                "    birthday TIMESTAMP(3),\n" +
                "    ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'hadoop102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'server-time-zone' = 'Asia/Shanghai',\n" +
                "    'database-name' = 'flink_test_db',\n" +
                "    'table-name' = 'users'\n" +
                ")\n";

//        tEnv.executeSql(sourceCDCSQL);
//        tEnv.executeSql("select * from mysql_users").print();


        String sinkSQL = "CREATE TABLE hudi_users\n" +
                "(\n" +
                "    id BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "    name STRING,\n" +
                "    birthday TIMESTAMP(3),\n" +
                "    ts TIMESTAMP(3),\n" +
                "    `partition` VARCHAR(20)\n" +
                ") PARTITIONED BY (`partition`) WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'path' = 'hdfs://hadoop102:8020/user/warehouse/hudi_users',\n" +
                "    'read.streaming.enabled' = 'true',\n" +
                "    'read.streaming.check-interval' = '1' \n" +
                ")";



        String insertSQL = "INSERT INTO hudi_users SELECT *, DATE_FORMAT(birthday, 'yyyyMMdd') FROM mysql_users";


        tEnv.executeSql(sourceCDCSQL);
        tEnv.executeSql(sinkSQL);
        tEnv.executeSql(insertSQL);






    }

}
