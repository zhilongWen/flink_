package com.at.paimon

import org.apache.flink.streaming.api.functions.source.SourceFunction

object sql_insert_suite extends CommonSuite {
  def main(args: Array[String]): Unit = {

    val (env, tableEnv) = initStreamEnv()

    tableEnv.executeSql("show catalogs").print()
    tableEnv.executeSql("show current catalog").print()

    tableEnv.executeSql("use catalog fs_paimon_catalog")
    tableEnv.executeSql(
      """
        |CREATE TABLE fs_table_1 (
        |    user_id BIGINT,
        |    item_id BIGINT,
        |    behavior STRING,
        |    ts BIGINT,
        |    dt STRING,
        |    hh STRING,
        |    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
        |) PARTITIONED BY (dt, hh)
        |""".stripMargin
    )

    // {"user_id":1,"item_id":100,"behavior":"click","ts":1751618203242}
    // {"user_id":2,"item_id":100,"behavior":"search","ts":1751618203242}
    // {"user_id":3,"item_id":100,"behavior":"click","ts":1751618203242}
    tableEnv.executeSql(
      """
        |CREATE TABLE default_catalog.default_database.source_table (
        |    user_id BIGINT,
        |    item_id BIGINT,
        |    behavior STRING,
        |    ts BIGINT,
        |    event_time AS TO_TIMESTAMP_LTZ(ts, 3)
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'paimon_topic',
        |    'properties.group.id' = 'sql_insert_suite',
        |    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json'
        |)
        |""".stripMargin
    )

    tableEnv.executeSql("use catalog fs_paimon_catalog")

//    tableEnv.executeSql(
//      """
//        |-- 静态分区插入
//        | insert into fs_table_1 partition (dt='2022-05-01', hh='00')
//        | select user_id,
//        |      item_id,
//        |      behavior,
//        |      ts
//        | from default_catalog.default_database.source_table
//        | """.stripMargin
//    )
    tableEnv.executeSql(
      """
        |-- 动态分区插入，自动匹配分区
        |insert into fs_table_1
        |select user_id,
        |      item_id,
        |      behavior,
        |      ts,
        |      DATE_FORMAT(event_time, 'yyyy-MM-dd') as dt ,
        |      DATE_FORMAT(event_time, 'HH') as hh
        |from default_catalog.default_database.source_table
        |""".stripMargin
    )
  }
}
