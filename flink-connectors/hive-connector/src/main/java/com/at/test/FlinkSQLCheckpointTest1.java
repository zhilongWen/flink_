package com.at.test;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-08-07
 */
public class FlinkSQLCheckpointTest1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        env.setParallelism(4);


        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck1"));
        env.getCheckpointConfig().setCheckpointInterval(2 * 60 * 1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);






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
                + "    'topic' = 'hive-test-logs',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'hive-logs-group-id',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

        String sinkSQL = "CREATE TABLE kafka_sink_tbl (\n"
                + "    id bigint,\n"
                + "    name string,\n"
                + "    address string,\n"
                + "    ts bigint,\n"
                + "    dt string,\n"
                + "    hm string,\n"
                + "    mm string,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'upsert-kafka',\n"
                + "  'topic' = 'hive-logs',\n"
                + "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "  'key.format' = 'json',\n"
                + "  'key.json.ignore-parse-errors' = 'true',\n"
                + "  'value.format' = 'json',\n"
                + "  'value.json.fail-on-missing-field' = 'false',\n"
                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                + ")\n";

        String insertSQL = "insert into kafka_sink_tbl\n"
                + "select\n"
                + "    id,\n"
                + "    name,\n"
                + "    address,\n"
                + "    ts,\n"
                + "    date_format(row_time,'yyyyMMdd'),\n"
                + "    date_format(row_time,'HH'),\n"
                + "    date_format(row_time,'mm')\n"
                + "from kafka_source_tbl\n";

        tableEnv.executeSql(sourceSQL);
        tableEnv.executeSql("select * from kafka_source_tbl").print();

//        tableEnv.executeSql(sinkSQL);
//        tableEnv.executeSql(insertSQL);

//        env.execute();


    }

}
