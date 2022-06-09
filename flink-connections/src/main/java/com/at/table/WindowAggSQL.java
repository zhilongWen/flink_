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


    }

}
