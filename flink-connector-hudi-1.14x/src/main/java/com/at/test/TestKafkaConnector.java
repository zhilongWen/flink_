package com.at.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-15
 */
public class TestKafkaConnector {

    public static void main(String[] args) {


        Configuration configuration = new Configuration();
        configuration.setString("parallelism.default",String.valueOf(1));
        configuration.setString("rest.port",String.valueOf(8081));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("create table if not exists hudi_user_behavior_tbl(\n"
                + "     userId int,\n"
                + "     itemId bigint,\n"
                + "     categoryId int,\n"
                + "     behavior string,\n"
                + "     ts bigint,\n"
                + "     row_time as to_timestamp(from_unixtime(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n"
                + "     watermark for row_time as row_time - interval '1' second\n"
                + ")with(\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'user_behaviors',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-group-id',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")");

        tableEnv.executeSql("select\n"
                + "    userId as user_id,\n"
                + "    itemId as item_id,\n"
                + "    categoryId as category_id,\n"
                + "    behavior,\n"
                + "    ts,\n"
                + "    row_time,\n"
                + "    date_format(cast(row_time as string),'yyyyMMdd') dt,\n"
                + "    date_format(cast(row_time as string),'HH') hh,\n"
                + "    date_format(cast(row_time as string),'mm') mm\n"
                + "from hudi_user_behavior_tbl\n").print();


    }

}
