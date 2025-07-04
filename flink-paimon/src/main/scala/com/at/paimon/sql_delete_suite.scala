package com.at.paimon

import org.apache.flink.api.common.RuntimeExecutionMode

object sql_delete_suite extends CommonSuite {
  def main(args: Array[String]): Unit = {

    val (env, tableEnv) = initStreamEnv()


    tableEnv.executeSql("show catalogs").print()
    tableEnv.executeSql("show current catalog").print()

    tableEnv.executeSql("use catalog fs_paimon_catalog")

    // delete
    // 只有写入模式设置为 change-log 的表支持此功能。 (有主键默认就是 change-log)
    // 如果表有主键，MergeEngine 需要为 deduplicate。(默认 deduplicate)
    // 只支持批模式     val settings = EnvironmentSettings.newInstance.inBatchMode().build

    //        tableEnv.executeSql("select * from fs_table_1").print()
    // +----+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //| op |              user_id |              item_id |                       behavior |                   ts |                             dt |                             hh |
    //+----+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //| +I |                    3 |                  110 |                         search |        1751618213242 |                     2025-07-04 |                             16 |
    //| +I |                    2 |                  100 |                         search |        1751618203242 |                     2025-07-04 |                             16 |
    //| +I |                    1 |                  100 |                             pv |        1751618205242 |                     2025-07-04 |                             16 |
    //| +I |                    1 |                  100 |                             pv |        1751620261056 |                     2025-07-04 |                             17 |

    // Exception in thread "main" org.apache.flink.table.api.TableException: Unsupported query: delete from fs_table_1 where user_id = 1
    //    tableEnv.executeSql("delete from fs_table_1 where user_id = 1")
    //    tableEnv.executeSql("select * from fs_table_1").print()
    //+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //|              user_id |              item_id |                       behavior |                   ts |                             dt |                             hh |
    //+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //|                    2 |                  100 |                         search |        1751618203242 |                     2025-07-04 |                             16 |
    //|                    3 |                  110 |                         search |        1751618213242 |                     2025-07-04 |                             16 |
    //+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //2 rows in set

    //    tableEnv.executeSql("delete from fs_table_1 where user_id = 1 and dt = '2025-07-04' and hh = '16' ")


    tableEnv.executeSql("select * from fs_table_1").print()
    //+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //|              user_id |              item_id |                       behavior |                   ts |                             dt |                             hh |
    //+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
    //|                    2 |                  100 |                         search |        1751618203242 |                     2025-07-04 |                             16 |
    //|                    1 |                  100 |                             pv |        1751620261056 |                     2025-07-04 |                             17 |
    //|                    3 |                  110 |                         search |        1751618213242 |                     2025-07-04 |                             16 |
    //+----------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+

  }
}
