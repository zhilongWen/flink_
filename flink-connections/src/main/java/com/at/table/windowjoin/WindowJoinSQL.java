package com.at.table.windowjoin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @create 2022-06-09
 */
public class WindowJoinSQL {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Row> leftStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2020-04-15T12:02"),1,"L1"),
                        Row.of(LocalDateTime.parse("2020-04-15T12:06"),2,"L2"),
                        Row.of(LocalDateTime.parse("2020-04-15T12:03"),3,"L3")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Row>() {
                                    @Override
                                    public long extractTimestamp(Row element, long recordTimestamp) {
                                        java.time.LocalDateTime field = (java.time.LocalDateTime) element.getField(0);

                                        return field.toInstant(ZoneOffset.ofHours(+8)).toEpochMilli();
                                    }
                                }
                        )
                );


        DataStream<Row> rightStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2020-04-15T12:01"),2,"R2"),
                        Row.of(LocalDateTime.parse("2020-04-15T12:04"),3,"R3"),
                        Row.of(LocalDateTime.parse("2020-04-15T12:05"),4,"R4")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Row>() {
                                            @Override
                                            public long extractTimestamp(Row element, long recordTimestamp) {
                                                java.time.LocalDateTime field = (java.time.LocalDateTime) element.getField(0);

                                                return field.toInstant(ZoneOffset.ofHours(+8)).toEpochMilli();
                                            }
                                        }
                                )
                );


        Table leftTable = tableEnv
                .fromDataStream(
                        leftStream,
                        Schema
                                .newBuilder()
                                .column("f0", DataTypes.TIMESTAMP(3))
                                .column("f1", DataTypes.INT())
                                .column("f2", DataTypes.STRING())
                                .watermark("f0", "f0 - interval '1' second")
                                .build()
                )
                .as("row_time", "num", "id");
        tableEnv.createTemporaryView("left_table",leftTable);

        Table rightTable = tableEnv
                .fromDataStream(
                        rightStream,
                        Schema
                                .newBuilder()
                                .column("f0", DataTypes.TIMESTAMP(3))
                                .column("f1", DataTypes.INT())
                                .column("f2", DataTypes.STRING())
                                .watermark("f0", "f0 - interval '1' second")
                                .build()
                )
                .as("row_time", "num", "id");
        tableEnv.createTemporaryView("right_table",rightTable);


        tableEnv.executeSql("select row_time,num,id from left_table").print();
        tableEnv.executeSql("select row_time,num,id from right_table").print();

/*
left_table
+-------------------------+-------------+--------------------------------+
|                row_time |         num |                             id |
+-------------------------+-------------+--------------------------------+
| 2020-04-15 12:02:00.000 |           1 |                             L1 |
| 2020-04-15 12:06:00.000 |           2 |                             L2 |
| 2020-04-15 12:03:00.000 |           3 |                             L3 |
+-------------------------+-------------+--------------------------------+
right_table
+-------------------------+-------------+--------------------------------+
|                row_time |         num |                             id |
+-------------------------+-------------+--------------------------------+
| 2020-04-15 12:01:00.000 |           2 |                             R2 |
| 2020-04-15 12:04:00.000 |           3 |                             R3 |
| 2020-04-15 12:05:00.000 |           4 |                             R4 |
+-------------------------+-------------+--------------------------------+
 */

        String q1 = "select\n"
                + "    *\n"
                + "from\n"
                + "table(\n"
                + "    tumble(\n"
                + "        table left_table,\n"
                + "        descriptor(row_time),\n"
                + "        interval '5' minutes\n"
                + "    )\n"
                + ")";

        String q2 = "select\n"
                + "    *\n"
                + "from\n"
                + "table(\n"
                + "    tumble(\n"
                + "        table right_table,\n"
                + "        descriptor(row_time),\n"
                + "        interval '5' minutes\n"
                + "    )\n"
                + ")";

//        tableEnv.executeSql(q1).print();
//        tableEnv.executeSql(q2).print();
/*
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:02:00.000 |           1 |                             L1 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:06:00.000 |           2 |                             L2 | 2020-04-15 12:05:00.000 | 2020-04-15 12:10:00.000 | 2020-04-15 12:09:59.999 |
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
3 rows in set
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:01:00.000 |           2 |                             R2 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:04:00.000 |           3 |                             R3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:05:00.000 |           4 |                             R4 | 2020-04-15 12:05:00.000 | 2020-04-15 12:10:00.000 | 2020-04-15 12:09:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
 */

        String q3 = "select\n"
                + "    *\n"
                + "from  \n"
                + "(\n"
                + "    select\n"
                + "        *\n"
                + "    from\n"
                + "    table(\n"
                + "        tumble(\n"
                + "            table left_table,\n"
                + "                descriptor(row_time),\n"
                + "                interval '5' minutes\n"
                + "            )\n"
                + "        )\n"
                + ") L\n"
                + "full join \n"
                + "(\n"
                + "    select\n"
                + "        *\n"
                + "    from\n"
                + "    table(\n"
                + "        tumble(\n"
                + "            table right_table,\n"
                + "            descriptor(row_time),\n"
                + "            interval '5' minutes\n"
                + "        )\n"
                + "    )\n"
                + ") R\n"
                + "on L.num = R.num and L.window_start = R.window_start and L.window_end = R.window_end";

//        tableEnv.executeSql(q3).print();
/*
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |               row_time0 |        num0 |                            id0 |           window_start0 |             window_end0 |            window_time0 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:02:00.000 |           1 |                             L1 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> |
| 2020-04-15 12:06:00.000 |           2 |                             L2 | 2020-04-15 12:05:00.000 | 2020-04-15 12:10:00.000 | 2020-04-15 12:09:59.999 |                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> |
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 | 2020-04-15 12:04:00.000 |           3 |                             R3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
|                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> | 2020-04-15 12:01:00.000 |           2 |                             R2 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
|                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> | 2020-04-15 12:05:00.000 |           4 |                             R4 | 2020-04-15 12:05:00.000 | 2020-04-15 12:10:00.000 | 2020-04-15 12:09:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+

 */



    String q4 = "select\n"
            + "    *\n"
            + "from\n"
            + "(select * from table(tumble(table left_table,descriptor(row_time),interval '5' minutes))) L\n"
            + "where L.num in (\n"
            + "    select\n"
            + "        num\n"
            + "    from\n"
            + "         (select * from table(tumble(table right_table,descriptor(row_time),interval '5' minutes))) R \n"
            + "    where L.window_start = R.window_start and L.window_end = R.window_end\n"
            + ")";

//    tableEnv.executeSql(q4).print();
/*
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
 */



        String q5 = "select\n"
                + "    *\n"
                + "from  \n"
                + "(select * from table(tumble(table left_table,descriptor(row_time),interval '5' minutes))) L\n"
                + "where exists(\n"
                + "    select * from (select * from table(tumble(table right_table,descriptor(row_time),interval '5' minutes))) R\n"
                + "    where L.num = R.num and L.window_start = R.window_start and L.window_end = R.window_end\n"
                + ")";

//        tableEnv.executeSql(q5).print();

/*
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
 */

















    }

}
