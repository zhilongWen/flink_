package com.at.table;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @create 2022-05-28
 */
public class StreamToDynamicTable {

    public static void main(String[] args) throws Exception {

        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // stream source
        DataStreamSource<Tuple3<String, String, Long>> streamSource = env
                .fromElements(
                        Tuple3.of("Mary", "./home", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Bob", "./cart", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=1", 12 * 60 * 60 * 1000L + 5 * 1000L),
                        Tuple3.of("Liz", "./home", 12 * 60 * 60 * 1000L + 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=3", 12 * 60 * 60 * 1000L + 90 * 1000L),
                        Tuple3.of("Mary", "./prod?id=7", 12 * 60 * 60 * 1000L + 105 * 1000L)
                );

        // assign watermark
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream = streamSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        // stream -> dynamic table
        Table table = tableEnv
                .fromDataStream(
                        stream,
                        $("f0").as("user"),
                        $("f1").as("url"),
                        $("f2").rowtime().as("cTime")
                );

        // register temporary view
        tableEnv.createTemporaryView("TableView",table);

        // sql
//        tableEnv.executeSql("select * from TableView").print();

        Table result = tableEnv
                .sqlQuery("select user,count(url) from TableView group by user");


        // dynamic table -> stream
        // 查询结果转换成数据流 更新日志流（用于查询中有聚合操作的情况）
        DataStream<Row> resultStream = tableEnv.toChangelogStream(result);

        resultStream.print();


        env.execute();


    }

}
