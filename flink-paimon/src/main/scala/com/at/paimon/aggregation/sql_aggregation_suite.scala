package com.at.paimon.aggregation

import com.at.paimon.CommonSuite

object sql_aggregation_suite extends CommonSuite {
  def main(args: Array[String]): Unit = {

    val (env, tableEnv) = initStreamEnv()
    tableEnv.executeSql("use catalog fs_paimon_catalog")

    env.setParallelism(1)

    tableEnv.executeSql(
      """
        |CREATE TABLE if not exists  default_catalog.default_database.source_table (
        |  user_id BIGINT,
        |  item_id BIGINT,
        |  behavior STRING,
        |  click_count BIGINT,
        |  view_time BIGINT,
        |  ts BIGINT,
        |  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
        |  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'paimon_aggregation_topic',
        |    'properties.group.id' = 'sql_aggregation_suite',
        |    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json'
        |)
        |""".stripMargin
    )

    //  /* 定义聚合函数：
    //     - total_clicks: 累加点击次数
    //     - total_view_time: 累加浏览时间
    //     - last_click_time: 保留最新时间戳
    //     - item_ids: 收集所有点击过的商品ID */
    //  /* 注意：需要在 WITH 子句中指定字段级的聚合函数 */
    tableEnv.executeSql(
      """
        |CREATE TABLE if not exists user_behavior_aggregations (
        |  user_id BIGINT PRIMARY KEY NOT ENFORCED,
        |  total_clicks BIGINT,
        |  total_view_time BIGINT,
        |  last_click_time TIMESTAMP(3),
        |  item_ids ARRAY<BIGINT>
        |) WITH (
        |  'connector' = 'paimon',
        |  'bucket' = '4',
        |  -- 'primary-key' = 'user_id',      Cannot define primary key on DDL and table options at the same time.
        |  'merge-engine' = 'aggregation',
        |  'field.total_clicks.aggregation' = 'sum',      -- 累加器
        |  'field.total_view_time.aggregation' = 'sum',  -- 累加器
        |  'field.last_click_time.aggregation' = 'max',  -- 最大值
        |  'field.item_ids.aggregation' = 'list-append',  -- 列表追加
        |  'changelog-producer' = 'full-compaction'  -- 完整压实模式
        |)
        |""".stripMargin)

    tableEnv.executeSql(
      """
        |-- 从源表插入数据到聚合表
        |INSERT INTO user_behavior_aggregations
        |SELECT
        |  user_id,
        |  click_count as total_clicks,
        |  view_time as total_view_time,
        |  event_time as last_click_time,
        |  ARRAY[item_id]  -- 将当前商品ID包装为数组
        |FROM default_catalog.default_database.source_table
        |""".stripMargin)

    // {"user_id":1,"item_id":100,"behavior":"click","click_count":3,"view_time":1,"ts":1751640841660}
    // {"user_id":1,"item_id":100,"behavior":"click","click_count":2,"view_time":10,"ts":1751640851660}
    // {"user_id":1,"item_id":101,"behavior":"click","click_count":10,"view_time":2,"ts":1751640951660}

    // user_id   total_clicks   total_view_time   last_click_time       item_ids
    // 1         15             13               2022-05-05 14:00:51.660  [100, 101]

  }
}
