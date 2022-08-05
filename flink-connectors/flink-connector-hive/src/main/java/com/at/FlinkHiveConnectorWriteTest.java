package com.at;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-08-05
 */
public class FlinkHiveConnectorWriteTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("testdb");


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


        String sourceSQL = "create table if not exists  kafka_source_tbl(\n"
                + "    id bigint,\n"
                + "    name string,\n"
                + "    address string,\n"
                + "    ts bigint,\n"
                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000),'yyyy-MM-dd HH:mm:ss'),\n"
                + "    watermark for row_time as row_time - interval '10' second \n"
                + ")with(\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'hive-logs',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'hive-logs-group-id',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select * from kafka_source_tbl").print();



    }

}
