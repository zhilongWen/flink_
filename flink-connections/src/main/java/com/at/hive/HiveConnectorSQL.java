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

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String sourceSQL = "create table file_soure_tbl\n"
                + "(\n"
                + "    user_id     bigint,\n"
                + "    item_id     bigint,\n"
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

        tableEnv.executeSql(sourceSQL);

        tableEnv.executeSql("select * from file_soure_tbl").print();


    }


}
