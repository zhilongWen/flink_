package com.at.testp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-08-07
 */
public class FlinkSQLCheckpointTest {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.setString("rest.port","8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.setParallelism(4);

        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck1"));
        env.getCheckpointConfig().setCheckpointInterval(2 * 60 * 1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().enableUnalignedCheckpoints(true);

//        // 设置状态后端
//        env.setStateBackend(new org.apache.flink.runtime.state.filesystem.FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck1"));
//        // 每隔 10min 做一次 checkpoint 模式为 AT_LEAST_ONCE
//        env.enableCheckpointing(1 * 60 * 1000L, CheckpointingMode.AT_LEAST_ONCE);
//
//
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        // 设置 checkpoint 最小间隔周期 1min
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        // 设置 checkpoint 必须在 1min 内完成，否则会被丢弃
//        checkpointConfig.setCheckpointTimeout(60 * 1000L);
//        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
//        checkpointConfig.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
//        // 设置 checkpoint 的并发度为 1
//        checkpointConfig.setMaxConcurrentCheckpoints(1);


        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\flink_\\files\\ck1"));


        // 启用检查点，指定触发checkpoint的时间间隔（单位：毫秒，默认500毫秒），默认情况是不开启的
//        env.enableCheckpointing(1000L);
//         设定语义模式，默认情况是exactly_once
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 设定Checkpoint超时时间，默认为10分钟
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多，
//        // 最终Flink应用密切触发Checkpoint操作，会占用了大量计算资源而影响到整个应用的性能（单位：毫秒）
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        // 默认情况下，只有一个检查点可以运行
//        // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 外部检查点
//        // 不会在任务正常停止的过程中清理掉检查点数据，而是会一直保存在外部系统介质中，另外也可以通过从外部检查点中对任务进行恢复
////    env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 如果有更近的保存点时，是否将作业回退到该检查
////    env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
//        // 设置可以允许的checkpoint失败数
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);


//        String name            = "myhive";
//        String defaultDatabase = "default";
//        String hiveConfDir     = "./conf";
//        String version = "3.1.2";
//
//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
//        tableEnv.registerCatalog("myhive", hive);
//
//        // set the HiveCatalog as the current catalog of the session
//        tableEnv.useCatalog("myhive");
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.useDatabase("testdb");


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
//                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";

//        tableEnv.executeSql(sourceSQL);
//
//        tableEnv.executeSql("select\n"
//                + "    id,\n"
//                + "    name,\n"
//                + "    address,\n"
//                + "    ts,\n"
//                + "    date_format(row_time,'yyyyMMdd'),\n"
//                + "    date_format(row_time,'HH'),"
//                + "    date_format(row_time,'mm')\n"
//                + "from kafka_source_tbl").print();

//        String sinkSQL = "CREATE TABLE if not exists  kafka_sink_tbl (\n"
//                + "    id bigint,\n"
//                + "    name string,\n"
//                + "    address string,\n"
//                + "    ts bigint,\n"
//                + "    dt string,\n"
//                + "    hm string,\n"
//                + "    mm string,\n"
//                + "  PRIMARY KEY (id) NOT ENFORCED\n"
//                + ") WITH (\n"
//                + "  'connector' = 'upsert-kafka',\n"
//                + "  'topic' = 'hive-logs',\n"
//                + "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
//                + "  'format' = 'json'\n"
//                + ")";

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
        tableEnv.executeSql(sinkSQL);
        tableEnv.executeSql(insertSQL);

        env.execute();


    }

}
