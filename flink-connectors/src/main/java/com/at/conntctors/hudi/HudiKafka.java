package com.at.conntctors.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-14
 */
public class HudiKafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        tableEnv.executeSql("CREATE TABLE if not exists order_kafka_source\n"
                + "(\n"
                + "    orderId STRING,\n"
                + "    userId STRING,\n"
                + "    orderTime STRING,\n"
                + "    ip STRING,\n"
                + "    orderMoney  DOUBLE,\n"
                + "    orderStatus INT\n"
                + ")\n"
                + "WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'order-topic',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-group-id',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.fail-on-missing-field' = 'false',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")");

//        tableEnv.executeSql("SELECT orderId, userId, orderTime, ip, orderMoney, orderStatus FROM order_kafka_source").print();


        tableEnv.executeSql("CREATE TABLE order_hudi_sink (\n"
                + "     orderId STRING PRIMARY KEY NOT ENFORCED,\n"
                + "     userId STRING,\n"
                + "     orderTime STRING,\n"
                + "     ip STRING,\n"
                + "     orderMoney DOUBLE,\n"
                + "     orderStatus INT,\n"
                + "     ts STRING,\n"
                + "     partition_day STRING\n"
                + ")\n"
                + "    PARTITIONED BY (partition_day)\n"
                + "WITH (\n"
                + "    'connector' = 'hudi',\n"
                + "    'path' = 'file:///D:/workspace/flink_/files/order_hudi_sink',\n"
                + "    'table.type' = 'MERGE_ON_READ',\n"
                + "    'write.operation' = 'upsert',\n"
                + "    'hoodie.datasource.write.recordkey.field'= 'orderId',\n"
                + "    'write.precombine.field' = 'ts',\n"
                + "    'write.tasks'= '1'\n"
                + ")");

        tableEnv.executeSql("INSERT INTO order_hudi_sink\n"
                + "SELECT\n"
                + "    orderId, userId, orderTime, ip, orderMoney, orderStatus,\n"
                + "    substring(orderId, 0, 17) AS ts, substring(orderTime, 0, 10) AS partition_day\n"
                + "FROM order_kafka_source ");


        tableEnv.executeSql("SELECT * FROM order_hudi_sink").print();





    }

}
