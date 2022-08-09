package com.at.writehive;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.concurrent.TimeUnit;

/**
 * @create 2022-08-08
 */
public class FlinkWriteHive111 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                // 任务失败的时间启动的间隔
                Time.of(5, TimeUnit.SECONDS),
                // 允许任务延迟时间 3s
                Time.of(5, TimeUnit.SECONDS))
        );



        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck"));
        env.getCheckpointConfig().setCheckpointInterval(1 * 60 * 1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("testdb");


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String sourceSQL = "create table if not exists kafka_source_tbl(\n"
                + "   id bigint,\n"
                + "   name string,\n"
                + "   address string,\n"
                + "   ts bigint,\n"
                + "   row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000),'yyyy-MM-dd HH:mm:ss'),\n"
                + "   watermark for row_time as row_time - interval '60' second\n"
                + ")with(\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'hive-test-logs',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'hive-logs-group-id',\n"
//                + "    'scan.startup.mode' = 'timestamp',\n"
//                + "    'scan.startup.timestamp-millis' = '1659800291596',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
//                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";


        tableEnv.executeSql(sourceSQL);
        tableEnv.executeSql("select\n"
                + "    id,\n"
                + "    name,\n"
                + "    address,\n"
                + "    ts,\n"
                + "    date_format(row_time,'yyyy-MM-dd'),\n"
                + "    date_format(row_time,'HH'),\n"
                + "    date_format(row_time,'mm')\n"
                + "from kafka_source_tbl").print();



        String hiveSinkSQL = "create table if not exists hive_stream_create_flink_tbl(\n"
                + "       id bigint,\n"
                + "       name string,\n"
                + "       address string,\n"
                + "       ts bigint\n"
                + ")COMMENT 'flink create table test'\n"
                + "    PARTITIONED BY (`dt` STRING,`hm` STRING,`mm` STRING)\n"
                + "    STORED AS ORC\n"
                + "    LOCATION '/warehouse/test/hive_stream_create_flink_tbl'\n"
                + "    TBLPROPERTIES (\n"
                + "        'orc.compress' = 'snappy',\n"
                + "        'partition.time-extractor.timestamp-pattern'='$dt $hr:$mm:00',\n"
                + "        'sink.partition-commit.trigger'='partition-time',\n"
                + "        'sink.partition-commit.delay'='1 min',\n"
                + "        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n"
                + "        'sink.partition-commit.policy.kind'='metastore,success-file'\n"
                + "    )";


        String insertSQL = "insert into hive_stream_create_flink_tbl\n"
                + "select\n"
                + "    id,\n"
                + "    name,\n"
                + "    address,\n"
                + "    ts,\n"
                + "    date_format(row_time,'yyyy-MM-dd'),\n"
                + "    date_format(row_time,'HH'),\n"
                + "    date_format(row_time,'mm')\n"
                + "from kafka_source_tbl\n";


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.executeSql(sourceSQL);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql(hiveSinkSQL);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.executeSql(insertSQL);






    }

}
