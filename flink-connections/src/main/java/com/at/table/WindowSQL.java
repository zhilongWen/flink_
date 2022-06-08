package com.at.table;

import com.at.util.EnvironmentUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.print.DocFlavor;
import java.time.LocalDateTime;

/**
 * @create 2022-06-08
 */
public class WindowSQL {

    public static void main(String[] args) throws Exception {

        // --execute.mode batch --enable.table.env true -Xms 50m -Xmx 50m --default.parallelism 1

        EnvironmentUtil.Environment environment = EnvironmentUtil.getExecutionEnvironment(args);

        StreamExecutionEnvironment env = environment.getEnv();
        StreamTableEnvironment tableEnv = environment.getTableEnv();


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        DataStream<Row>  streamSource =  env
                .fromElements(
                        Row.of(LocalDateTime.parse("2020-04-15T08:05:00"), 4.00, "C"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:07:00"), 2.00, "A"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:09:00"), 5.00, "D"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:11:00"), 3.00, "B"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:13:00"), 1.00, "E"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:17:00"), 6.00, "F")
                ).returns(
                Types.ROW_NAMED(
                        new String[]{"bidtime", "price", "item"},
                        Types.LOCAL_DATE_TIME,
                        Types.DOUBLE,
                        Types.STRING
                ));

        Schema schema = Schema
                .newBuilder()
                .column("bidtime", DataTypes.TIMESTAMP(3))
                .column("price", DataTypes.DOUBLE())
                .column("item", DataTypes.STRING())
                .watermark("bidtime","bidtime - interval '1' SECOND")
                .build();

        Table table = tableEnv
                .fromDataStream(streamSource, schema);
//                .as("bidtime", "price", "item");

        tableEnv.createTemporaryView("bid_tbl", table);


        String querySQL_1 = "select\n"
                + "    *\n"
                + "from\n"
                + "table(\n"
                + "    TUMBLE(\n"
                + "        TABLE bid_tbl, \n"
                + "        DESCRIPTOR(bidtime),\n"
                + "        INTERVAL '10' MINUTES\n"
                + "    )\n"
                + ")";

//        tableEnv.executeSql(querySQL_1).print();
/*
+----+-------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| op |                 bidtime |                          price |                           item |            window_start |              window_end |             window_time |
+----+-------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| +I | 2020-04-15 08:05:00.000 |                            4.0 |                              C | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |
| +I | 2020-04-15 08:07:00.000 |                            2.0 |                              A | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |
| +I | 2020-04-15 08:09:00.000 |                            5.0 |                              D | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |
| +I | 2020-04-15 08:11:00.000 |                            3.0 |                              B | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |
| +I | 2020-04-15 08:13:00.000 |                            1.0 |                              E | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |
| +I | 2020-04-15 08:17:00.000 |                            6.0 |                              F | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |
+----+-------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------------------+
 */

        String querySQL_2 = "SELECT\n"
                + "       *\n"
                + "FROM\n"
                + "TABLE(\n"
                + "    TUMBLE(\n"
                + "        DATA => TABLE bid_tbl,\n"
                + "        TIMECOL => DESCRIPTOR(bidtime),\n"
                + "        SIZE => INTERVAL '10' MINUTES\n"
                + "    )\n"
                + ")";

//        tableEnv.executeSql(querySQL_2).print();


        String querySQL_3 = "SELECT \n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    sum(price)\n"
                + "from\n"
                + "table(\n"
                + "    tumble(\n"
                + "        table bid_tbl,\n"
                + "        descriptor(bidtime), \n"
                + "        interval '10' minutes\n"
                + "    )\n"
                + ")\n"
                + "group by window_start, window_end";

//        tableEnv.executeSql(querySQL_3).print();

/*
+----+-------------------------+-------------------------+--------------------------------+
| op |            window_start |              window_end |                         EXPR$2 |
+----+-------------------------+-------------------------+--------------------------------+
| +I | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |
| +I | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                           10.0 |
+----+-------------------------+-------------------------+--------------------------------+
 */


        String hop = "SELECT \n"
                + "    *\n"
                + "FROM  \n"
                + "TABLE(\n"
                + "    HOP(\n"
                + "        TABLE bid_tbl,\n"
                + "        DESCRIPTOR(bidtime),\n"
                + "        INTERVAL '5' MINUTES,\n"
                + "        INTERVAL '10' MINUTES\n"
                + "    )    \n"
                + ")";

//        tableEnv.executeSql(hop).print();
/*
+----+-------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| op |                 bidtime |                          price |                           item |            window_start |              window_end |             window_time |
+----+-------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| +I | 2020-04-15 08:05:00.000 |                            4.0 |                              C | 2020-04-15 08:05:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |
| +I | 2020-04-15 08:05:00.000 |                            4.0 |                              C | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |
| +I | 2020-04-15 08:07:00.000 |                            2.0 |                              A | 2020-04-15 08:05:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |
| +I | 2020-04-15 08:07:00.000 |                            2.0 |                              A | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |
| +I | 2020-04-15 08:09:00.000 |                            5.0 |                              D | 2020-04-15 08:05:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |
| +I | 2020-04-15 08:09:00.000 |                            5.0 |                              D | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |
| +I | 2020-04-15 08:11:00.000 |                            3.0 |                              B | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |
| +I | 2020-04-15 08:11:00.000 |                            3.0 |                              B | 2020-04-15 08:05:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |
| +I | 2020-04-15 08:13:00.000 |                            1.0 |                              E | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |
| +I | 2020-04-15 08:13:00.000 |                            1.0 |                              E | 2020-04-15 08:05:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |
| +I | 2020-04-15 08:17:00.000 |                            6.0 |                              F | 2020-04-15 08:15:00.000 | 2020-04-15 08:25:00.000 | 2020-04-15 08:24:59.999 |
| +I | 2020-04-15 08:17:00.000 |                            6.0 |                              F | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |
+----+-------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------------------+
 */

        String hopAgg = "SELECT\n"
                + "    window_start,\n"
                + "    window_end,\n"
                + "    sum(price)\n"
                + "FROM\n"
                + "TABLE(\n"
                + "    HOP(\n"
                + "        TABLE bid_tbl,\n"
                + "        DESCRIPTOR(bidtime),\n"
                + "        INTERVAL '5' MINUTES,\n"
                + "        INTERVAL '10' MINUTES\n"
                + "    )\n"
                + ")\n"
                + "GROUP BY window_start,window_end";

        tableEnv.executeSql(hopAgg).print();

/*
+----+-------------------------+-------------------------+--------------------------------+
| op |            window_start |              window_end |                         EXPR$2 |
+----+-------------------------+-------------------------+--------------------------------+
| +I | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |
| +I | 2020-04-15 08:05:00.000 | 2020-04-15 08:15:00.000 |                           15.0 |
| +I | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                           10.0 |
| +I | 2020-04-15 08:15:00.000 | 2020-04-15 08:25:00.000 |                            6.0 |
+----+-------------------------+-------------------------+--------------------------------+
 */

        env.execute();


    }

}
