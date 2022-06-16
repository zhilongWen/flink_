package com.at.hudi;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-06-15
 */
public class JdbcMOR {

/*
20:57:47,247 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220616205745234] error
org.apache.hudi.hive.HoodieHiveSyncException: Got runtime exception when hive syncing
	at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:100) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:89) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.sink.utils.HiveSyncContext.hiveSyncTool(HiveSyncContext.java:57) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.doSyncHive(StreamWriteOperatorCoordinator.java:337) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:93) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142) [?:1.8.0_131]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617) [?:1.8.0_131]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_131]
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Failed to create HiveMetaStoreClient
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:89) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:95) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	... 7 more
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Cannot create hive connection jdbc:hive2://hadoop102:1000/
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:104) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:56) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:79) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:95) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	... 7 more
Caused by: java.sql.SQLException: Could not open client transport with JDBC Uri: jdbc:hive2://hadoop102:1000: java.net.ConnectException: Connection refused: connect
	at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:256) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at java.sql.DriverManager.getConnection(DriverManager.java:664) ~[?:1.8.0_131]
	at java.sql.DriverManager.getConnection(DriverManager.java:247) ~[?:1.8.0_131]
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:101) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:56) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:79) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:95) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	... 7 more
Caused by: org.apache.thrift.transport.TTransportException: java.net.ConnectException: Connection refused: connect
	at org.apache.thrift.transport.TSocket.open(TSocket.java:226) ~[libthrift-0.9.3.jar:0.9.3]
	at org.apache.thrift.transport.TSaslTransport.open(TSaslTransport.java:266) ~[libthrift-0.9.3.jar:0.9.3]
	at org.apache.thrift.transport.TSaslClientTransport.open(TSaslClientTransport.java:37) ~[libthrift-0.9.3.jar:0.9.3]
	at org.apache.hive.jdbc.HiveConnection.openTransport(HiveConnection.java:343) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:228) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at java.sql.DriverManager.getConnection(DriverManager.java:664) ~[?:1.8.0_131]
	at java.sql.DriverManager.getConnection(DriverManager.java:247) ~[?:1.8.0_131]
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:101) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:56) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:79) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:95) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	... 7 more
Caused by: java.net.ConnectException: Connection refused: connect
	at java.net.DualStackPlainSocketImpl.connect0(Native Method) ~[?:1.8.0_131]
	at java.net.DualStackPlainSocketImpl.socketConnect(DualStackPlainSocketImpl.java:79) ~[?:1.8.0_131]
	at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350) ~[?:1.8.0_131]
	at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206) ~[?:1.8.0_131]
	at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188) ~[?:1.8.0_131]
	at java.net.PlainSocketImpl.connect(PlainSocketImpl.java:172) ~[?:1.8.0_131]
	at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392) ~[?:1.8.0_131]
	at java.net.Socket.connect(Socket.java:589) ~[?:1.8.0_131]
	at org.apache.thrift.transport.TSocket.open(TSocket.java:221) ~[libthrift-0.9.3.jar:0.9.3]
	at org.apache.thrift.transport.TSaslTransport.open(TSaslTransport.java:266) ~[libthrift-0.9.3.jar:0.9.3]
	at org.apache.thrift.transport.TSaslClientTransport.open(TSaslClientTransport.java:37) ~[libthrift-0.9.3.jar:0.9.3]
	at org.apache.hive.jdbc.HiveConnection.openTransport(HiveConnection.java:343) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:228) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at java.sql.DriverManager.getConnection(DriverManager.java:664) ~[?:1.8.0_131]
	at java.sql.DriverManager.getConnection(DriverManager.java:247) ~[?:1.8.0_131]
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:101) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:56) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:79) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:95) ~[hudi-flink1.14-bundle_2.12-0.11.0.jar:0.11.0]
	... 7 more
 */
    public static void main(String[] args) {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive",hive);


        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");


        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String sourceSQL = "create table if not exists hudi_user_behavior_tbl\n"
                + "(\n"
                + "    userId     int,\n"
                + "    itemId     bigint,\n"
                + "    categoryId int,\n"
                + "    behavior string,\n"
                + "    ts         bigint,\n"
                + "    row_time   as to_timestamp(from_unixtime(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n"
                + "    watermark for row_time as row_time - interval '1' second\n"
                + ")\n"
                + "with (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'user_behaviors',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'properties.group.id' = 'test-group-id',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";


        String hudiSinkSQL = "create table if not exists user_behavior_jdbc_mor\n"
                + "(\n"
                + "    user_id     int,\n"
                + "    item_id     bigint,\n"
                + "    category_id int,\n"
                + "    behavior string,\n"
                + "    ts          bigint,\n"
                + "    `dt` string,\n"
                + "    `hh` string,\n"
                + "    `mm` string\n"
                + ")partitioned by (`dt`,`hh`,`mm`)\n"
                + "with(\n"
                + "    'connector'='hudi',\n"
                + "    'path'='hdfs://hadoop102:8020/user/warehouse/user_behavior_jdbc_mor_tbl', -- hdfs上存储位置\n"
                + "    'table.type'='MERGE_ON_READ',    -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出\n"
                + "    'hoodie.datasource.write.recordkey.field' = 'user_id', -- 主键\n"
                + "    'write.precombine.field'= 'ts',  -- 自动precombine的字段\n"
                + "    'write.tasks'='1',\n"
                + "    'write.rate.limit'= '100', -- 限速\n"
                + "    'compaction.async.enabled'= 'true', -- 异步压缩\n"
                + "    'compaction.tasks'='1',\n"
                + "    'compaction.trigger.strategy'= 'num_and_time', -- 按次数和时间压缩\n"
                + "    'compaction.delta_commits'= '1', -- 默认为5\n"
                + "    'read.streaming.enabled' = 'true',  -- 开启stream mode\n"
                + "    'read.streaming.check-interval' = '4',  -- 检查间隔，默认60s\n"
                + "    'hive_sync.mode' = 'jdbc',            -- required, 将hive sync mode设置为hms, 默认jdbc\n"
                + "    'hive_sync.enable'='true',           -- required，开启hive同步功能\n"
                + "    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083' ,\n"
                + "    'hive_sync.table'='user_behavior_jdbc_mor_tbl',                          -- required, hive 新建的表名\n"
                + "    'hive_sync.db'='default',                       -- required, hive 新建的数据库名\n"
                + "    'hive_sync.support_timestamp'= 'true',  -- 兼容hive timestamp类型\n"
                + "    -- jdbc\n"
                + "    'hive_sync.jdbc_url'='jdbc:hive2://hadoop102:1000', -- required, hiveServer port\n"
                + "    'hive_sync.username'='jdbc:hive2://hadoop102:10000',                -- required, JDBC username\n"
                + "    'hive_sync.password'='zero'                  -- required, JDBC password\n"
                + ")\n";


        String insertSQL ="insert into user_behavior_jdbc_mor\n"
                + "select\n"
                + "    userId as user_id,\n"
                + "    itemId as item_id,\n"
                + "    categoryId as category_id,\n"
                + "    behavior,\n"
                + "    ts,\n"
                + "    date_format(cast(row_time as string),'yyyyMMdd') dt,\n"
                + "    date_format(cast(row_time as string),'HH') hh,\n"
                + "    date_format(cast(row_time as string),'mm') mm\n"
                + "from hudi_user_behavior_tbl";

        tableEnv.executeSql(sourceSQL);
        tableEnv.executeSql(hudiSinkSQL);
        tableEnv.executeSql(insertSQL);


    }


}
