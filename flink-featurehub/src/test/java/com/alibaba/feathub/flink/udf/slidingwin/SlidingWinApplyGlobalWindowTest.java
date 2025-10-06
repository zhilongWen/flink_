package com.alibaba.feathub.flink.udf.slidingwin;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;
import com.alibaba.feathub.flink.udf.SlidingWindowDescriptor;
import com.alibaba.feathub.flink.udf.SlidingWindowUtils;
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
public class SlidingWinApplyGlobalWindowTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        DataStream<UserBehavior> sourceStream = createTestDataSource(env);
        Table sourceTable = createSourceTable(tableEnv, sourceStream);

        tableEnv.createTemporaryView("source_view", sourceTable);
//        tableEnv.executeSql("select * from source_view").print();

        SlidingWindowDescriptor windowDescriptor = new SlidingWindowDescriptor(
                Duration.ZERO,
                Arrays.asList("userId")
        );

        AggregationFieldsDescriptor aggDescriptors = AggregationFieldsDescriptor.builder()
                .addField("click_count", DataTypes.BIGINT(), DataTypes.BIGINT(),
                        0L, null, "behaviorType = 'click'", "COUNT")
                .addField("total_value", DataTypes.BIGINT(), DataTypes.BIGINT(),
                        0L, null, null, "SUM")
                .build();

        Table processedTable = sourceTable;
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            if ("click_count".equals(descriptor.fieldName)) {
                processedTable = processedTable.addOrReplaceColumns($("behaviorType").as(descriptor.fieldName));
            } else {
                processedTable = processedTable.addOrReplaceColumns($("value").as(descriptor.fieldName));
            }
        }

        Table result = SlidingWindowUtils.applyGlobalWindow(
                tableEnv, processedTable, windowDescriptor, aggDescriptors, "event_time");
        result.printSchema();

        tableEnv.createTemporaryView("result_view", result);
        tableEnv.executeSql("select * from result_view").print();

        env.execute();
    }

    private static Table createSourceTable(StreamTableEnvironment tableEnv, DataStream<UserBehavior> sourceStream) {
        Schema schema = Schema.newBuilder()
                .column("userId", DataTypes.STRING())
                .column("behaviorType", DataTypes.STRING())
                .column("value", DataTypes.BIGINT())
                .column("timestamp", DataTypes.BIGINT())
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(`timestamp`, 3)")
                .watermark("event_time", "event_time - INTERVAL '2' SECOND")
                .build();

        Table table = tableEnv.fromDataStream(sourceStream, schema);
        table.printSchema();

        return table;
    }

    private static DataStream<UserBehavior> createTestDataSource(StreamExecutionEnvironment env) {
        return env.addSource(new SourceFunction<UserBehavior>() {
            private final List<String> behaviorTypes = Arrays.asList("pv", "click", "like", "purchase");
            private final Random random = new Random();
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<UserBehavior> ctx) throws Exception {
                while (isRunning) {
                    String userId = "user_" + random.nextInt(5);
                    String behaviorType = behaviorTypes.get(random.nextInt(behaviorTypes.size()));
                    long value = random.nextInt(100) + 1;
                    long timestamp = System.currentTimeMillis();

                    ctx.collect(new UserBehavior(userId, behaviorType, value, timestamp));
                    Thread.sleep(1000); // 快速生成测试数据
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
    }
}
