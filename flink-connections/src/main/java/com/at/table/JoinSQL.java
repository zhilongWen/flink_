package com.at.table;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * @create 2022-06-09
 */
public class JoinSQL {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        DataStream<Row> orderSource = env
                .fromElements(
                        Row.of("90001", 10.09, "EUR", LocalDateTime.parse("2022-06-09T12:00:00")),
                        Row.of("90002", 15.50, "Dollar", LocalDateTime.parse("2022-06-09T11:56:06"))
                )
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"order_id", "price", "currency", "order_time"},
                                Types.STRING,
                                Types.DOUBLE,
                                Types.STRING,
                                Types.LOCAL_DATE_TIME
                        )
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Row>() {
                                            @Override
                                            public long extractTimestamp(Row element, long recordTimestamp) {
                                                LocalDateTime file = (LocalDateTime) element.getField("order_time");
                                                return file.toInstant(ZoneOffset.ofHours(+8)).toEpochMilli();
                                            }
                                        }
                                )
                );

        DataStream<Row> shipmentSource = env
                .fromElements(
                        Row.of("90001", "十月稻田 长粒香大米 东北大米 东北香米 5kg等3件商品", LocalDateTime.parse("2022-06-09T12:09:00")),
                        Row.of("90002", "Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米等5件商品", LocalDateTime.parse("2022-06-09T11:50:05")),
                        Row.of("90003", "索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 等8件商品", LocalDateTime.parse("2022-06-09T12:01:20")),
                        Row.of("90001", "TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视等7件商品", LocalDateTime.parse("2022-06-09T13:01:00")),
                        Row.of("90001", "小米电视4A 70英寸 4K超高清 HDR 二级能效 2GB+16GB L70M5-4A 内置小爱 智能网络液晶平板教育电视等5件商品", LocalDateTime.parse("2022-07-09T11:50:56"))
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(0l))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Row>() {
                                            @Override
                                            public long extractTimestamp(Row element, long recordTimestamp) {

                                                LocalDateTime field = (LocalDateTime) element.getField(2);

                                                return field.toInstant(ZoneOffset.ofHours(+8)).toEpochMilli();

                                            }
                                        }));

        DataStream<Row> rateSource = env
                .fromElements(
                        Row.of("Dollar", 102.00),
                        Row.of("Euro", 114.00),
                        Row.of("Yen ", 1.00)
                );

        Table orderTable = tableEnv
                .fromDataStream(
                        orderSource,
                        Schema
                                .newBuilder()
                                .column("order_id", DataTypes.STRING())
                                .column("price", DataTypes.DOUBLE())
                                .column("currency", DataTypes.STRING())
                                .column("order_time", DataTypes.TIMESTAMP(3))
                                .watermark("order_time", "order_time - INTERVAL '1' SECOND")
                                .build()
                ).as("order_id", "price", "currency", "order_time");
        tableEnv.createTemporaryView("order_table", orderTable);


        Table shipTable = tableEnv
                .fromDataStream(
                        shipmentSource,
                        Schema
                                .newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.STRING())
                                .column("f2", DataTypes.TIMESTAMP(3))
                                .watermark("f2", "f2 - interval '1' second")
                                .build()
                ).as("id", "desc", "ship_time");
        tableEnv.createTemporaryView("ship_table", shipTable);

        Table rateTable = tableEnv
                .fromDataStream(
                        rateSource,
                        Schema
                                .newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.DOUBLE())
                                .build()
                ).as("currency", "rate");
        tableEnv.createTemporaryView("rate_table", rateTable);


//        tableEnv.executeSql("select * from order_table").print();
//        tableEnv.executeSql("select * from ship_table").print();
//        tableEnv.executeSql("select * from rate_table").print();

/*

order_table
+--------------------------------+--------------------------------+--------------------------------+-------------------------+
|                       order_id |                          price |                       currency |              order_time |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |
|                          90002 |                           15.5 |                         Dollar | 2022-06-09 11:56:06.000 |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+

ship_table
+--------------------------------+--------------------------------+-------------------------+
|                             id |                           desc |               ship_time |
+--------------------------------+--------------------------------+-------------------------+
|                          90001 |  十月稻田 长粒香大米 东北大... | 2022-06-09 12:00:00.000 |
|                          90002 |  Redmi 10X 4G Helio G85游戏... | 2022-06-09 11:56:06.000 |
|                          90003 | 索芙特i-Softto 口红不掉色唇... | 2022-06-09 12:01:20.000 |
|                          90001 | TCL 75Q10 75英寸 QLED原色量... | 2022-06-09 13:01:00.000 |
|                          90001 | 小米电视4A 70英寸 4K超高清 ... | 2022-06-09 13:05:56.000 |
+--------------------------------+--------------------------------+-------------------------+

rate_table
+--------------------------------+--------------------------------+
|                       currency |                           rate |
+--------------------------------+--------------------------------+
|                         Dollar |                          102.0 |
|                           Euro |                          114.0 |
|                           Yen  |                            1.0 |
+--------------------------------+--------------------------------+

 */


        String innerJoinSQL = "SELECT\n"
                + "    *\n"
                + "FROM order_table\n"
                + "INNER JOIN ship_table\n"
                + "ON order_table.order_id = ship_table.id";

//        tableEnv.executeSql(innerJoinSQL).print();
/*
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
|                       order_id |                          price |                       currency |              order_time |                             id |                           desc |               ship_time |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 |  十月稻田 长粒香大米 东北大... | 2022-06-09 12:00:00.000 |
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 | TCL 75Q10 75英寸 QLED原色量... | 2022-06-09 13:01:00.000 |
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 | 小米电视4A 70英寸 4K超高清 ... | 2022-06-09 13:05:56.000 |
|                          90002 |                           15.5 |                         Dollar | 2022-06-09 11:56:06.000 |                          90002 |  Redmi 10X 4G Helio G85游戏... | 2022-06-09 11:56:06.000 |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
 */

        String leftJoinSQL = "SELECT\n"
                + "    *\n"
                + "FROM order_table ot\n"
                + "LEFT JOIN ship_table st\n"
                + "ON ot.order_id = st.id";

//        tableEnv.executeSql(leftJoinSQL).print();
/*
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
|                       order_id |                          price |                       currency |              order_time |                             id |                           desc |               ship_time |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 |  十月稻田 长粒香大米 东北大... | 2022-06-09 12:00:00.000 |
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 | TCL 75Q10 75英寸 QLED原色量... | 2022-06-09 13:01:00.000 |
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 | 小米电视4A 70英寸 4K超高清 ... | 2022-06-09 13:05:56.000 |
|                          90002 |                           15.5 |                         Dollar | 2022-06-09 11:56:06.000 |                          90002 |  Redmi 10X 4G Helio G85游戏... | 2022-06-09 11:56:06.000 |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
 */

        String fullJoinSQL = "SELECT\n"
                + "    *\n"
                + "FROM order_table ot\n"
                + "FULL OUTER JOIN ship_table st\n"
                + "ON ot.order_id = st.id";

//        tableEnv.executeSql(fullJoinSQL).print();
/*
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
|                       order_id |                          price |                       currency |              order_time |                             id |                           desc |               ship_time |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 |  十月稻田 长粒香大米 东北大... | 2022-06-09 12:00:00.000 |
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 | TCL 75Q10 75英寸 QLED原色量... | 2022-06-09 13:01:00.000 |
|                          90001 |                          10.09 |                            EUR | 2022-06-09 12:00:00.000 |                          90001 | 小米电视4A 70英寸 4K超高清 ... | 2022-06-09 13:05:56.000 |
|                          90002 |                           15.5 |                         Dollar | 2022-06-09 11:56:06.000 |                          90002 |  Redmi 10X 4G Helio G85游戏... | 2022-06-09 11:56:06.000 |
|                         <NULL> |                         <NULL> |                         <NULL> |                  <NULL> |                          90003 | 索芙特i-Softto 口红不掉色唇... | 2022-06-09 12:01:20.000 |
+--------------------------------+--------------------------------+--------------------------------+-------------------------+--------------------------------+--------------------------------+-------------------------+
 */


        String intervalJionSQL = "SELECT\n"
                + "    *\n"
                + "FROM order_table ot\n"
                + "RIGHT JOIN ship_table st\n"
                + "ON ot.order_id = st.id\n"
                + "AND st.ship_time BETWEEN ot.order_time - INTERVAL '10' MINUTE AND ot.order_time + INTERVAL '10' MINUTE";

        tableEnv.executeSql(innerJoinSQL).print();





        env.execute();


    }

}
