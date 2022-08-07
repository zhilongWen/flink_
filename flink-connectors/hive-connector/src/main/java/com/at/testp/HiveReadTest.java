package com.at.testp;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-08-07
 */
public class HiveReadTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

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


        tableEnv.executeSql("select * from tt").print();


        env.execute();

    }

}
