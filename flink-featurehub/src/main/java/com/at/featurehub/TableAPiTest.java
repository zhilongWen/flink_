package com.at.featurehub;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;


public class TableAPiTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Stream 执行环境（流处理核心环境）
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建 StreamTableEnvironment（关联 Stream 环境，必须用这个！）
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        // 3. 定义你的 DataStreamSource<String>（原代码不变）
        DataStreamSource<String> streamSource = streamEnv.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect("a#1");
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
            }
        });

        Schema schema = Schema.newBuilder()
                .column("f0", "STRING")
                .column("f1", "INT")
                .columnByExpression("pt", "PROCTIME()")
                .build();

        Table sourceTable = tableEnv.fromDataStream(
                streamSource.map(
                        rawStr -> {
                            String[] parts = rawStr.split("#");
                            return Row.of(parts[0], Integer.parseInt(parts[1]));
                        },
                        Types.ROW(Types.STRING, Types.INT)
                ),
                schema
        );

//        // 4. 核心 WordCount 逻辑（与批处理一致，Table API 链式调用）
//        Table resultTable = sourceTable
//                // 按单词分组
//                .groupBy($("f0"))
//                // 统计次数
//                .select($("f0"), $("f1").count().as("count"));
//
//        // 5. 流处理结果实时打印（toDataStream 会自动触发作业运行）
//        System.out.println("流处理 WordCount 结果（Table API 原生，持续输出）：");
////        tableEnv.toDataStream(resultTable, Row.class).print();
//        tableEnv.toChangelogStream(resultTable)
//                .map(row -> {
//                    RowKind kind = row.getKind(); // 获取操作类型
//                    String f0 = row.getFieldAs(0);
//                    Long count = row.getFieldAs(1);
//                    return String.format("Kind: %s, f0: %s, count: %d", kind, f0, count);
//                })
//                .print();

        Table resultTable = sourceTable
                .window(Tumble.over(lit(5).second()).on($("pt")).as("w"))
                .groupBy($("f0"), $("w"))
                .select(
                        $("f0"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("f1").count().as("count")
                );

        tableEnv.toDataStream(resultTable, Row.class).print();

        streamEnv.execute("Flink 1.20 DataStream to Table");
    }
}
