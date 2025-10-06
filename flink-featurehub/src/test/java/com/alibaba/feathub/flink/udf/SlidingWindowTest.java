package com.alibaba.feathub.flink.udf;

import com.alibaba.feathub.flink.udf.bean.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: wenzhilong
 * @Email: wenzhilong@bilibili.com
 * @Date: 2025/10/06
 * @Desc:
 */
public class SlidingWindowTest {
    public static void main(String[] args) throws Exception {
        // 使用简单的本地环境，不启动Web UI
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        DataStream<UserBehavior> sourceStream = getDataStream(env);
        // 注意：字段名必须与UserBehavior类的getter方法匹配
        // getUserId() -> userId, getBehaviorType() -> behaviorType, etc.
        Schema schema = Schema.newBuilder()
                .column("userId", DataTypes.STRING())
                .column("behaviorType", DataTypes.STRING())
                .column("value", DataTypes.BIGINT())
                .column("timestamp", DataTypes.BIGINT())
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(`timestamp`, 3)")
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build();

        Table table = tableEnv.fromDataStream(sourceStream, schema);
        table.printSchema();

        tableEnv.createTemporaryView("source_table", table);
//        tableEnv.executeSql("select * from source_table").print();

        SlidingWindowDescriptor windowDescriptor =
                new SlidingWindowDescriptor(Duration.ZERO, Arrays.asList("userId", "behaviorType"));

        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField(
                                "val_sum",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                0L,
                                null,
                                null,
                                "SUM")
                        .addField(
                                "val_count",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                0L,
                                null,
                                null,
                                "COUNT")
                        .build();

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            table = table.addOrReplaceColumns($("value").as(descriptor.fieldName));
        }

        Table res_tbl =
                SlidingWindowUtils.applyGlobalWindow(
                        tableEnv, table, windowDescriptor, aggDescriptors, "event_time");

        tableEnv.createTemporaryView("res_tbl", res_tbl);
        tableEnv.executeSql("select * from res_tbl").print();
    }

    private static DataStream<UserBehavior> getDataStream(StreamExecutionEnvironment env) {
        return env.addSource(new SourceFunction<UserBehavior>() {

            List<String> behaviorTypes = Arrays.asList("pv", "uv", "click", "like");
            Random random = new Random();

            @Override
            public void run(SourceContext<UserBehavior> ctx) throws Exception {
                while (true) {
                    int userId = random.nextInt(10);
                    long value = random.nextLong(100);
                    String behaviorType = behaviorTypes.get(random.nextInt(behaviorTypes.size()));

//                    Row row = Row.withNames();
//                    row.setField("user_id", "user_" + userId);
//                    row.setField("behavior_type", behaviorType);
//                    row.setField("value", value);
//                    row.setField("timestamp", System.currentTimeMillis());
//                    ctx.collect(row);

                    ctx.collect(new UserBehavior("user_" + userId, behaviorType, value, System.currentTimeMillis()));

                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
    }
}
