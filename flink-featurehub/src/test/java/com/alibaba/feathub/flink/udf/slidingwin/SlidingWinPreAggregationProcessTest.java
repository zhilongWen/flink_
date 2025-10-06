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
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: wenzhilong
 * @Email: wenzhilong@bilibili.com
 * @Date: 2025/10/06
 * @Desc:
 */
public class SlidingWinPreAggregationProcessTest {
    // -XX:+UseG1GC -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -XX:+IgnoreUnrecognizedVMOptions -Dlog4j2.formatMsgNoLookups=true --add-opens=java.base/java.net.URI=ALL-UNNAMED --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED --add-opens=java.base/sun.net.util=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=UTF-8 --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
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

        // 创建滑动窗口描述符 - 30秒滑动步长
        SlidingWindowDescriptor windowDescriptor = new SlidingWindowDescriptor(
                Duration.ofSeconds(30),
                Arrays.asList("userId")
        );

        // 创建聚合字段描述符
        AggregationFieldsDescriptor aggDescriptors = AggregationFieldsDescriptor.builder()
                .addField("pv_count", DataTypes.BIGINT(), DataTypes.BIGINT(),
                        Duration.ofMinutes(5).toMillis(), null, "behaviorType = 'pv'", "COUNT")
                .addField("click_count", DataTypes.BIGINT(), DataTypes.BIGINT(),
                        Duration.ofMinutes(3).toMillis(), null, "behaviorType = 'click'", "COUNT")
                .addField("total_value", DataTypes.BIGINT(), DataTypes.BIGINT(),
                        Duration.ofMinutes(5).toMillis(), null, null, "SUM")
                .build();

        // 为聚合字段添加源数据
        Table processedTable = sourceTable;
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            if ("pv_count".equals(descriptor.fieldName) || "click_count".equals(descriptor.fieldName)) {
                processedTable = processedTable.addOrReplaceColumns($("behaviorType").as(descriptor.fieldName));
            } else {
                processedTable = processedTable.addOrReplaceColumns($("value").as(descriptor.fieldName));
            }
        }

        // 调用applySlidingWindowPreAggregationProcess
        DataStream<Row> preAggResult = SlidingWindowUtils.applySlidingWindowPreAggregationProcess(
                tableEnv, processedTable, windowDescriptor, aggDescriptors, "event_time");

        Table preAggTable = tableEnv.fromDataStream(preAggResult);
        System.out.println("=== 预聚合结果Schema ===");
        preAggTable.printSchema();

        System.out.println("=== 注意: 预聚合结果包含Accumulator状态，不能直接打印 ===");
        System.out.println("预聚合成功！结果包含以下累加器状态:");
        System.out.println("- pv_count: CountAccumulator");
        System.out.println("- click_count: CountAccumulator");
        System.out.println("- total_value: SumAccumulator");
        System.out.println("这些状态将在下一阶段(SlidingWindowAggregationProcess)中处理为最终值");


        System.out.println("\\n=== 测试下一阶段: SlidingWindowAggregationProcess ===");
        // 构建数据类型映射
        Map<String, DataType> dataTypeMap = new HashMap<>();
        dataTypeMap.put("userId", DataTypes.STRING());
        dataTypeMap.put("pv_count", DataTypes.BIGINT());
        dataTypeMap.put("click_count", DataTypes.BIGINT());
        dataTypeMap.put("total_value", DataTypes.BIGINT());
        dataTypeMap.put("event_time", DataTypes.TIMESTAMP_LTZ(3));

        // 调用滑动窗口聚合处理
        Table finalResult = SlidingWindowUtils.applySlidingWindowAggregationProcess(
                tableEnv,
                preAggResult,
                dataTypeMap,
                windowDescriptor,
                "event_time",
                aggDescriptors,
                null, // 不使用零值行
                false // 不跳过相同窗口输出
        );

        System.out.println("最终聚合结果Schema:");
        finalResult.printSchema();
        tableEnv.createTemporaryView("result_view", finalResult);
        tableEnv.executeSql("select * from result_view").print();
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
