package com.at.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * @create 2022-06-09
 */
public class WindowAggSQL {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        DataStreamSource<Row> streamSource = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2020-04-15T08:05:00"), 4.00, "C", "supplier1"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:07:00"), 2.00, "A", "supplier1"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:09:00"), 5.00, "D", "supplier2"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:11:00"), 3.00, "B", "supplier2"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:13:00"), 1.00, "E", "supplier1"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:17:00"), 6.00, "F", "supplier2")
                );

        Schema schema = Schema
                .newBuilder()
                .column("f0", DataTypes.TIMESTAMP(3))
                .column("f1", DataTypes.DOUBLE())
                .column("f2", DataTypes.STRING())
                .column("f3", DataTypes.STRING())
                .watermark("f0", "f0 - interval '1' second")
                .build();

        Table table = tableEnv
                .fromDataStream(streamSource, schema)
                .as("bidtime", "price", "item", "supplier_id");

        tableEnv.createTemporaryView("bid_tbl",table);


        tableEnv.executeSql("desc bid_tbl").print();
        tableEnv.executeSql("select * from bid_tbl").print();


        String tumbleAggSQL = "SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    sum(price)\n"
                + "FROM\n"
                + "TABLE\n"
                + "(\n"
                + "    TUMBLE(\n"
                + "        TABLE bid_tbl,\n"
                + "        DESCRIPTOR(bidtime),\n"
                + "        INTERVAL  '10' MINUTES\n"
                + "    )\n"
                + ")\n"
                + "GROUP BY window_start,window_end";

//        tableEnv.executeSql(tumbleAggSQL).print();
/*
+-------------------------+-------------------------+--------------------------------+
|            window_start |              window_end |                         EXPR$2 |
+-------------------------+-------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                           10.0 |
+-------------------------+-------------------------+--------------------------------+
 */

        String hopAggSQL = "SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    sum(price)\n"
                + "FROM\n"
                + "TABLE\n"
                + "(\n"
                + "    HOP(\n"
                + "        TABLE bid_tbl,\n"
                + "        DESCRIPTOR(bidtime),\n"
                + "        INTERVAL '2' MINUTES,\n"
                + "        INTERVAL '10' MINUTES\n"
                + "    )\n"
                + ")\n"
                + "GROUP BY window_start,window_end";

//        tableEnv.executeSql(hopAggSQL).print();

/*
+-------------------------+-------------------------+--------------------------------+
|            window_start |              window_end |                         EXPR$2 |
+-------------------------+-------------------------+--------------------------------+
| 2020-04-15 08:04:00.000 | 2020-04-15 08:14:00.000 |                           15.0 |
| 2020-04-15 08:02:00.000 | 2020-04-15 08:12:00.000 |                           14.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |
| 2020-04-15 07:58:00.000 | 2020-04-15 08:08:00.000 |                            6.0 |
| 2020-04-15 07:56:00.000 | 2020-04-15 08:06:00.000 |                            4.0 |
| 2020-04-15 08:06:00.000 | 2020-04-15 08:16:00.000 |                           11.0 |
| 2020-04-15 08:08:00.000 | 2020-04-15 08:18:00.000 |                           15.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                           10.0 |
| 2020-04-15 08:12:00.000 | 2020-04-15 08:22:00.000 |                            7.0 |
| 2020-04-15 08:16:00.000 | 2020-04-15 08:26:00.000 |                            6.0 |
| 2020-04-15 08:14:00.000 | 2020-04-15 08:24:00.000 |                            6.0 |
+-------------------------+-------------------------+--------------------------------+
 */

        String groupSetAggSQL = "SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    supplier_id,\n"
                + "    sum(price) as price\n"
                + "FROM\n"
                + "    TABLE\n"
                + "        (\n"
                + "            TUMBLE(\n"
                + "                TABLE bid_tbl,\n"
                + "                    DESCRIPTOR(bidtime),\n"
                + "                    INTERVAL  '10' MINUTES\n"
                + "                )\n"
                + "        )\n"
                + "GROUP BY window_start,window_end,GROUPING SETS((supplier_id),())";

//        tableEnv.executeSql(groupSetAggSQL).print();
/*
+-------------------------+-------------------------+--------------------------------+--------------------------------+
|            window_start |              window_end |                    supplier_id |                          price |
+-------------------------+-------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier1 |                            6.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                           11.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier2 |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier2 |                            9.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                           10.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier1 |                            1.0 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+
 */

        String cubeAggSQL1 = "SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    item,\n"
                + "    supplier_id,\n"
                + "    sum(price)\n"
                + "FROM\n"
                + "    TABLE\n"
                + "        (\n"
                + "            TUMBLE(\n"
                + "                TABLE bid_tbl,\n"
                + "                    DESCRIPTOR(bidtime),\n"
                + "                    INTERVAL  '10' MINUTES\n"
                + "                )\n"
                + "        )\n"
                + "GROUP BY window_start,window_end,\n"
                + "    GROUPING SETS(\n"
                + "    (supplier_id, item),\n"
                + "    (supplier_id      ),\n"
                + "    (             item),\n"
                + "    (                 )\n"
                + "    )";

//        tableEnv.executeSql(cubeAggSQL1).print();
/*
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
|            window_start |              window_end |                           item |                    supplier_id |                         EXPR$4 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                      supplier1 |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier1 |                            6.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                         <NULL> |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                         <NULL> |                           11.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                      supplier1 |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                         <NULL> |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                         <NULL> |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                      supplier2 |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier2 |                            9.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                         <NULL> |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                         <NULL> |                           10.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                         <NULL> |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                      supplier2 |                            6.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                         <NULL> |                            6.0 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
 */

        String cubeAggSQL = "SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    item,\n"
                + "    supplier_id,\n"
                + "    sum(price)\n"
                + "FROM\n"
                + "    TABLE\n"
                + "        (\n"
                + "            TUMBLE(\n"
                + "                TABLE bid_tbl,\n"
                + "                    DESCRIPTOR(bidtime),\n"
                + "                    INTERVAL  '10' MINUTES\n"
                + "                )\n"
                + "        )\n"
                + "GROUP BY window_start,window_end, CUBE (supplier_id, item)";

//        tableEnv.executeSql(cubeAggSQL).print();
/*
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
|            window_start |              window_end |                           item |                    supplier_id |                         EXPR$4 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                      supplier1 |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier1 |                            6.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                         <NULL> |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                         <NULL> |                           11.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                      supplier1 |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                         <NULL> |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                         <NULL> |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                      supplier2 |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier2 |                            9.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                         <NULL> |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                         <NULL> |                           10.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                         <NULL> |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                      supplier2 |                            6.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                         <NULL> |                            6.0 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
 */



        String cascadingSQL = "SELECT\n"
                + "    window_start as window_5mintumble_start,\n"
                + "    window_end as window_5mintumble_end,\n"
                + "    window_time as rowtime,\n"
                + "    SUM(price) as partial_price\n"
                + "FROM\n"
                + "TABLE(\n"
                + "    TUMBLE(\n"
                + "        TABLE bid_tbl,\n"
                + "        DESCRIPTOR(bidtime),\n"
                + "        INTERVAL '5' MINUTE\n"
                + "    )\n"
                + ")\n"
                + "GROUP BY supplier_id,window_start,window_end,window_time";

//        tableEnv.executeSql(cascadingSQL).print();
/*
+-------------------------+-------------------------+-------------------------+--------------------------------+
| window_5mintumble_start |   window_5mintumble_end |                 rowtime |                  partial_price |
+-------------------------+-------------------------+-------------------------+--------------------------------+
| 2020-04-15 08:05:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |                            6.0 |
| 2020-04-15 08:05:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |                            1.0 |
| 2020-04-15 08:15:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |                            6.0 |
+-------------------------+-------------------------+-------------------------+--------------------------------+
 */





    }

}
