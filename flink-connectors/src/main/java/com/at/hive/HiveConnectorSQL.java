package com.at.hive;

import com.at.constant.PropertiesConstants;
import com.at.util.EnvironmentUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-07
 */
public class HiveConnectorSQL {



    public static void main(String[] args) throws Exception {

        // --execute.mode batch --enable.checkpoint false --enable.table.env true --enable.hive.env true

        EnvironmentUtil.Environment environment = EnvironmentUtil.getExecutionEnvironment(args);

        StreamExecutionEnvironment env = environment.getEnv();
        StreamTableEnvironment tableEnv = environment.getTableEnv();

        ParameterTool parameters = (ParameterTool) env.getConfig().getGlobalJobParameters();

        if (EnvironmentUtil.checkArgument(parameters.get(PropertiesConstants.ENABLE_HIVE_ENV))) {
            tableEnv = EnvironmentUtil.enableHiveEnv(tableEnv);
        }



        String sourceSQL = "create table if not exists file_soure_tbl\n"
                + "(\n"
                + "    user_id     bigint,\n"
                + "    item_id     int,\n"
                + "    category_id bigint,\n"
                + "    behavior string,\n"
                + "    ts          bigint,\n"
                + "    row_time    as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')),\n"
                + "    watermark for row_time as row_time - interval '1' second\n"
                + ")\n"
                + "with (\n"
                + "    'connector' = 'filesystem',\n"
                + "    'path' = 'file:///D:/workspace/flink_/files/UserBehavior.csv',\n"
                + "    'format' = 'csv',\n"
                + "    'csv.ignore-parse-errors' = 'true',\n"
                + "    'csv.allow-comments' = 'true'\n"
                + ")\n";


        String sinkSQL = "create table if not exists user_behavior_tbl\n"
                + "(\n"
                + "    user_id     bigint,\n"
                + "    item_id     int,\n"
                + "    category_id bigint,\n"
                + "    behavior string,\n"
                + "    ts          bigint\n"
                + ") comment 'UserBehavior.csv' partitioned by (`dt` string,`hm` string)\n"
                + "    stored as orc\n"
                + "    location '/user/hive/warehouse/user_behavior_tbl'\n"
                + "    tblproperties(\n"
                + "        'orc.compress' = 'snappy',\n"
                + "        'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n"
                + "        'sink.partition-commit.trigger'='partition-time',\n"
                + "        'sink.partition-commit.delay'='5 min',\n"
                + "        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- Assume user configured time zone is 'Asia/Shanghai'\n"
                + "        'sink.partition-commit.policy.kind'='metastore,success-file'\n"
                + "    )";

        String insertSQL = "insert into user_behavior_tbl\n"
                + "select user_id,\n"
                + "       item_id,\n"
                + "       category_id,\n"
                + "       behavior,\n"
                + "       ts,\n"
                + "       DATE_FORMAT(row_time,'yyyyMMdd'),\n"
                + "       DATE_FORMAT(row_time,'HH')\n"
                + "from file_soure_tbl";

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("drop table if exists file_soure_tbl");
        tableEnv.executeSql(sourceSQL);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(sinkSQL);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(insertSQL);

//        tableEnv.executeSql("select count(*) from user_behavior_tbl").print();



    }


}
