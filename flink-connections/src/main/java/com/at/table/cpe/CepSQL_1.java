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
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @create 2022-06-09
 */
public class CepSQL_1 {

    //https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/cep-statements

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        DataStream<Row> cepStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2018-04-13T12:00:00"), "1", "Beijing", "Consumption"),
                        Row.of(LocalDateTime.parse("2018-04-13T12:05:00"), "1", "Shanghai", "Consumption"),
                        Row.of(LocalDateTime.parse("2018-04-13T12:10:00"), "1", "Shenzhen", "Consumption"),
                        Row.of(LocalDateTime.parse("2018-04-13T12:20:00"), "1", "Beijing", "Consumption")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Row>() {
                                            @Override
                                            public long extractTimestamp(Row element, long recordTimestamp) {
                                                LocalDateTime field = (LocalDateTime) element.getField(0);

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
                                .column("f0", DataTypes.TIMESTAMP(3))
                                .column("f1", DataTypes.STRING())
                                .column("f2", DataTypes.STRING())
                                .column("f3", DataTypes.STRING())
                                .watermark("f0", "f0 - interval '1' second")
                                .build()
                )
                .as("timestamp", "card_id", "location", "action");
        tableEnv.createTemporaryView("cep_table", cepTable);

        tableEnv.executeSql("select * from cep_table").print();

        tableEnv
                .executeSql("select \n"
                        + "`start_timestamp`, \n"
                        + "`end_timestamp`, \n"
                        + "card_id, `event`\n"
                        + "from cep_table\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "    PARTITION BY card_id   --按card_id分区，将相同卡号的数据分发到同一个计算节点。\n"
                        + "    ORDER BY `timestamp`   --在窗口内，对事件时间进行排序。\n"
                        + "    MEASURES               --定义如何根据匹配成功的输入事件构造输出事件。\n"
                        + "        e2.`action` as `event`,   \n"
                        + "        e1.`timestamp` as `start_timestamp`,   --第一次的事件时间为start_timestamp。\n"
                        + "        LAST(e2.`timestamp`) as `end_timestamp` --最新的事件时间为end_timestamp。\n"
                        + "    ONE ROW PER MATCH           --匹配成功输出一条。\n"
                        + "    AFTER MATCH SKIP TO NEXT ROW --匹配后跳转到下一行。\n"
                        + "    PATTERN (e1 e2+) WITHIN INTERVAL '10' MINUTE  --定义两个事件，e1和e2。\n"
                        + "    DEFINE                     --定义在PATTERN中出现的patternVariable的具体含义。\n"
                        + "        e1 as e1.action = 'Consumption',    --事件一的action标记为Consumption。\n"
                        + "        e2 as e2.action = 'Consumption' and e2.location <> e1.location --事件二的action标记为Consumption，且事件一和事件二的location不一致。\n"
                        + ")").print();


    }

}
