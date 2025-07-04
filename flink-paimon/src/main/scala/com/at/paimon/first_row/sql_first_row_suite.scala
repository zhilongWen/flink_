package com.at.paimon.first_row

import com.at.paimon.CommonSuite

object sql_first_row_suite extends CommonSuite {
  def main(args: Array[String]): Unit = {

    val (env, tableEnv) = initStreamEnv()

    tableEnv.executeSql("show catalogs").print()
    tableEnv.executeSql("show current catalog").print()

    tableEnv.executeSql("use catalog fs_paimon_catalog")

    tableEnv.executeSql(
      """
        |CREATE TABLE if not exists default_catalog.default_database.source_table (
        |    user_id BIGINT,
        |    item_id BIGINT,
        |    behavior STRING,
        |    ts BIGINT,
        |    event_time AS TO_TIMESTAMP_LTZ(ts, 3)
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'paimon_topic',
        |    'properties.group.id' = 'sql_first_row_suite',
        |    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json'
        |)
        |""".stripMargin
    )

    tableEnv.executeSql(
      """
        |CREATE TABLE if not exists fs_first_row_tbl (
        |  user_id BIGINT,
        |  item_id BIGINT,
        |  behavior STRING,
        |  ts TIMESTAMP(3),
        |  dt STRING,
        |  hh STRING,
        |  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- 设置水位线处理迟到数据
        |) WITH (
        |  'connector' = 'paimon',
        |  'bucket' = '4',
        |  'primary-key' = 'user_id, item_id',
        |  'file.format' = 'parquet'
        |)
        |""".stripMargin
    )

    tableEnv.executeSql(
      """
        |CREATE TABLE if not exists fs_first_click_per_user (
        |  user_id BIGINT PRIMARY KEY NOT ENFORCED,
        |  first_item_id BIGINT,
        |  first_click_time TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'paimon',
        |  'bucket' = '4',
        |  'file.format' = 'parquet'
        |)
        |""".stripMargin
    )

    // sql
    //    tableEnv.executeSql("""BEGIN STATEMENT SET""")
    //    tableEnv.executeSql("""END""")

    // 使用 Statement Set 统一提交多个 Sink
    val stmtSet = tableEnv.createStatementSet()

    stmtSet.addInsertSql(
      """
        |-- 动态分区插入，自动匹配分区
        |insert into fs_first_row_tbl
        |select user_id,
        |      item_id,
        |      behavior,
        |      event_time as ts,
        |      DATE_FORMAT(event_time, 'yyyy-MM-dd') as dt ,
        |      DATE_FORMAT(event_time, 'HH') as hh
        |from default_catalog.default_database.source_table
        |""".stripMargin
    )

//    stmtSet.addInsertSql(
//      """
//        |INSERT INTO fs_first_click_per_user
//        |    SELECT
//        |      user_id,
//        |      FIRST_ROW(item_id ORDER BY ts ASC) AS first_item_id,
//        |      FIRST_ROW(ts ORDER BY ts ASC) AS first_click_time
//        |    FROM fs_first_row_tbl
//        |    WHERE behavior = 'click'
//        |    GROUP BY user_id
//        |""".stripMargin
//    )

    stmtSet.addInsertSql(
      """
        |INSERT INTO fs_first_click_per_user
        |SELECT user_id, first_item_id, first_click_time
        |FROM (
        |  SELECT
        |    user_id,
        |    item_id AS first_item_id,
        |    ts AS first_click_time,
        |    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts ASC) AS rn
        |  FROM fs_first_row_tbl
        |  WHERE behavior = 'click'
        |) t
        |WHERE rn = 1""".stripMargin
    )

    // 统一提交作业（两个 Sink 共享同一源，单作业运行）
    stmtSet.execute()

  }
}
