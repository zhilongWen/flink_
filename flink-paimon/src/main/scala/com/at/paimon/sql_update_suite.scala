package com.at.paimon

import com.at.paimon.sql_insert_suite.initStreamEnv

object sql_update_suite extends CommonSuite {
  def main(args: Array[String]): Unit = {

    val (env, tableEnv) = initStreamEnv()

    tableEnv.executeSql("show catalogs").print()
    tableEnv.executeSql("show current catalog").print()

    tableEnv.executeSql("use catalog fs_paimon_catalog")

    // update 只能针对主键表
    // 只有主键表支持此功能。不支持更新主键。
    // MergeEngine 需要deduplicate或 partial-update 才能支持此功能。 (默认 deduplicate)

    tableEnv.executeSql(
      """
        |create temporary view default_catalog.default_database.tmp_view as
        |SELECT
        |  1 AS user_id,
        |  100 AS item_id,
        |  'pv' AS behavior,
        |
        |  -- test 1 分区与 old 一致
        |  -- 1751618205242 AS ts,  -- 2025-07-04 16:36:45.242
        |  -- TO_TIMESTAMP_LTZ(1751618205242, 3) AS event_time
        |
        |  -- test 2 分区与 old 不一致
        |  1751620261056 as ts, -- 2025-07-04 17:11:01.056
        |  TO_TIMESTAMP_LTZ(1751620261056, 3) AS event_time
        |""".stripMargin
    )

    tableEnv.executeSql("use catalog fs_paimon_catalog")
    // old
    //              user_id              item_id                       behavior                   ts                             dt                             hh
    //                    1                  100                          click        1751618203242                     2025-07-04                             16
    // new 1
    //              user_id              item_id                       behavior                   ts                             dt                             hh
    //                    1                  100                             pv        1751618205242                     2025-07-04                             16
    // new 2
    //              user_id              item_id                       behavior                   ts                             dt                             hh
    //                    1                  100                             pv        1751618205242                     2025-07-04                             16
    //                    1                  100                             pv        1751620261056                     2025-07-04                             17
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
        |from default_catalog.default_database.tmp_view
        |""".stripMargin
    )
  }
}
