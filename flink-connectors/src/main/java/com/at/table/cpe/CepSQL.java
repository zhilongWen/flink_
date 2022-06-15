package com.at.table.cpe;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @create 2022-06-09
 */
public class CepSQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        DataStream<Row> cepStream = env
                .fromElements(
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:00"),12,1),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:01"),17,2),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:02"),19,1),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:03"),21,3),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:04"),25,2),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:05"),18,1),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:06"),15,1),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:07"),14,2),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:08"),24,2),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:09"),25,2),
                        Row.of("ACME",LocalDateTime.parse("2021-08-01T10:00:10"),19,1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Row>() {
                                            @Override
                                            public long extractTimestamp(Row element, long recordTimestamp) {
                                                java.time.LocalDateTime field = (java.time.LocalDateTime) element.getField(1);

                                                return field.toInstant(ZoneOffset.ofHours(+8)).toEpochMilli();
                                            }
                                        }
                                )
                );


        Table cepTable = tableEnv
                .fromDataStream(
                        cepStream,
                        Schema
                                .newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.TIMESTAMP(3))
                                .column("f2", DataTypes.INT())
                                .column("f3", DataTypes.INT())
                                .watermark("f1", "f1 - interval '1' second")
                                .build()
                )
                .as("symbol", "rowtime", "price","tax");
        tableEnv.createTemporaryView("cep_table",cepTable);

        tableEnv.executeSql("select * from cep_table").print();


        tableEnv.executeSql("SELECT *\n"
                + "FROM cep_table\n"
                + "    MATCH_RECOGNIZE (\n"
                + "        PARTITION BY symbol\n"
                + "        ORDER BY rowtime\n"
                + "        MEASURES\n"
                + "            START_ROW.rowtime AS start_tstamp,\n"
                + "            LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,\n"
                + "            LAST(PRICE_UP.rowtime) AS end_tstamp\n"
                + "        ONE ROW PER MATCH\n"
                + "        AFTER MATCH SKIP TO LAST PRICE_UP\n"
                + "        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)\n"
                + "        DEFINE\n"
                + "            PRICE_DOWN AS\n"
                + "                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR\n"
                + "                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),\n"
                + "            PRICE_UP AS\n"
                + "                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)\n"
                + "    ) MR");



    }

}
