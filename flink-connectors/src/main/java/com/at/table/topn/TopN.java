package com.at.table.topn;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * @create 2022-06-09
 */
public class TopN {

    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        DataStreamSource<Row> streamSource = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2020-04-15T08:05"),4.00,"A","supplier1"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:06"),4.00,"C","supplier2"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:07"),2.00,"G","supplier1"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:08"),2.00,"B","supplier3"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:09"),5.00,"D","supplier4"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:11"),2.00,"B","supplier3"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:13"),1.00,"E","supplier1"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:15"),3.00,"H","supplier2"),
                        Row.of(LocalDateTime.parse("2020-04-15T08:17"),6.00,"F","supplier5")
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


        tableEnv.executeSql("select * from bid_tbl").print();
/*
+-------------------------+--------------------------------+--------------------------------+--------------------------------+
|                 bidtime |                          price |                           item |                    supplier_id |
+-------------------------+--------------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:05:00.000 |                            4.0 |                              A |                      supplier1 |
| 2020-04-15 08:06:00.000 |                            4.0 |                              C |                      supplier2 |
| 2020-04-15 08:07:00.000 |                            2.0 |                              G |                      supplier1 |
| 2020-04-15 08:08:00.000 |                            2.0 |                              B |                      supplier3 |
| 2020-04-15 08:09:00.000 |                            5.0 |                              D |                      supplier4 |
| 2020-04-15 08:11:00.000 |                            2.0 |                              B |                      supplier3 |
| 2020-04-15 08:13:00.000 |                            1.0 |                              E |                      supplier1 |
| 2020-04-15 08:15:00.000 |                            3.0 |                              H |                      supplier2 |
| 2020-04-15 08:17:00.000 |                            6.0 |                              F |                      supplier5 |
+-------------------------+--------------------------------+--------------------------------+--------------------------------+
 */

//        tableEnv.executeSql("            select\n"
//                + "                window_start,\n"
//                + "                window_end,\n"
//                + "                supplier_id,\n"
//                + "                sum(price) as price,\n"
//                + "                count(*) as cnt\n"
//                + "            from\n"
//                + "                table(\n"
//                + "                        tumble(\n"
//                + "                                table bid_tbl,\n"
//                + "                                descriptor(bidtime),\n"
//                + "                                interval '10' minutes\n"
//                + "                            )\n"
//                + "                    ) group by window_start,window_end,supplier_id").print();


        String tumbleTopN = "select \n"
                + "    *\n"
                + "from\n"
                + "(\n"
                + "    select\n"
                + "        *,\n"
                + "        row_number() over (partition by window_start,window_end order by price desc) as rn\n"
                + "    from\n"
                + "        (\n"
                + "            select\n"
                + "                window_start,\n"
                + "                window_end,\n"
                + "                supplier_id,\n"
                + "                sum(price) as price,\n"
                + "                count(*) as cnt\n"
                + "            from\n"
                + "                table(\n"
                + "                        tumble(\n"
                + "                            table bid_tbl,\n"
                + "                                descriptor(bidtime),\n"
                + "                                interval '10' minutes\n"
                + "                            )\n"
                + "                    ) group by window_start,window_end,supplier_id\n"
                + "        )\n"
                + ") where rn <= 3";

//        tableEnv.executeSql(tumbleTopN).print();
/*
+-------------------------+-------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
|            window_start |              window_end |                    supplier_id |                          price |                  cnt |                   rn |
+-------------------------+-------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier1 |                            6.0 |                    2 |                    1 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier4 |                            5.0 |                    1 |                    2 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier2 |                            4.0 |                    1 |                    3 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier5 |                            6.0 |                    1 |                    1 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier2 |                            3.0 |                    1 |                    2 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier3 |                            2.0 |                    1 |                    3 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
 */

        tableEnv.executeSql("select\n"
                + "    *\n"
                + "from\n"
                + "(\n"
                + "    select\n"
                + "        bidtime,\n"
                + "        price,\n"
                + "        item,\n"
                + "        supplier_id,\n"
                + "        window_start,\n"
                + "        window_end,\n"
                + "        row_number() over (partition by window_start,window_end order by price desc ) as rn\n"
                + "    from\n"
                + "        table(\n"
                + "                tumble(\n"
                + "                    table bid_tbl,\n"
                + "                        descriptor(bidtime),\n"
                + "                        interval '10' minutes\n"
                + "                    )\n"
                + "            )\n"
                + ") where rn <= 3").print();

/*
+-------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+----------------------+
|                 bidtime |                          price |                           item |                    supplier_id |            window_start |              window_end |                   rn |
+-------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+----------------------+
| 2020-04-15 08:09:00.000 |                            5.0 |                              D |                      supplier4 | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                    1 |
| 2020-04-15 08:05:00.000 |                            4.0 |                              A |                      supplier1 | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                    2 |
| 2020-04-15 08:06:00.000 |                            4.0 |                              C |                      supplier2 | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                    3 |
| 2020-04-15 08:17:00.000 |                            6.0 |                              F |                      supplier5 | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                    1 |
| 2020-04-15 08:15:00.000 |                            3.0 |                              H |                      supplier2 | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                    2 |
| 2020-04-15 08:11:00.000 |                            2.0 |                              B |                      supplier3 | 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                    3 |
+-------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+----------------------+
 */


    }

}
