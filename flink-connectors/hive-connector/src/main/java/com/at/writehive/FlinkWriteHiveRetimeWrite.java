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

import java.util.concurrent.TimeUnit;

/**
 * @create 2022-08-08
 */
public class FlinkWriteHiveRetimeWrite {

    //https://blog.csdn.net/gakki_200/article/details/106413351?spm=1001.2101.3001.6661.1&utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-1-106413351-blog-120781200.pc_relevant_aa&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-1-106413351-blog-120781200.pc_relevant_aa&utm_relevant_index=1

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(200);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setParallelism(4);


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
                + "    'topic' = 'flink-retime-write-topic',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'flink-retime-write-topic-groupId',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"  //latest-offset  earliest-offset
//                + "    'scan.startup.mode' = 'timestamp',\n"
//                + "    'scan.startup.timestamp-millis' = '1660469738017',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

        String hiveSinkSQL = "create table if not exists hive_stream_create_retime_write_tbl(\n"
                + "       id bigint,\n"
                + "       name string,\n"
                + "       address string,\n"
                + "       ts bigint\n"
                + ")COMMENT 'flink create table test'\n"
                + "    PARTITIONED BY (`dt` STRING,`hm` STRING,`mm` STRING)\n"
                + "    STORED AS ORC\n"
                + "    LOCATION '/warehouse/test/hive_stream_create_retime_write_tbl'\n"
                + "    TBLPROPERTIES (\n"
                + "        'orc.compress' = 'snappy',\n"
                + "        'partition.time-extractor.timestamp-pattern'='$dt $hm:$mm:00',\n"
                + "        'partition.time-extractor.timestamp-formatter'='yyyyMMdd HH:mm:ss',\n"
                + "        'sink.partition-commit.trigger'='partition-time',\n"
                + "        'sink.partition-commit.delay'='1 min',\n"
                + "        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n"
                + "        'sink.partition-commit.policy.kind'='metastore,success-file',\n"
                + "        'sink.rolling-policy.file-size'='1MB',\n"
                + "        'sink.rolling-policy.rollover-interval'='1 min',\n"
                + "        'compaction.file-size'='1MB',\n"
                + "        'sink.rolling-policy.rollover-interval'='1 min'\n"
                + "    )";


        String insertSQL = "insert into hive_stream_create_retime_write_tbl\n"
                + "select\n"
                + "    id,\n"
                + "    name,\n"
                + "    address,\n"
                + "    ts,\n"
                + "    date_format(row_time,'yyyyMMdd'),\n" // 适用默认的 partition.time-extractor.timestamp-pattern 格式为 yyyy-MM-dd HH:mm:ss
                + "    date_format(row_time,'HH'),\n"
                + "    date_format(row_time,'mm')\n"
                + "from kafka_source_tbl";


        tableEnv.executeSql(sourceSQL);

//        tableEnv.executeSql("select * from kafka_source_tbl").print();

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql(hiveSinkSQL);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.executeSql(insertSQL);





    }

}
