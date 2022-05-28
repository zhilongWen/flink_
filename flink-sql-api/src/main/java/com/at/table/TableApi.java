package com.at.table;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @create 2022-05-28
 */
public class TableApi {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("D:\\workspace\\flink_\\files\\UserBehavior.csv")
                .map(new RichMapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] elems = value.split(",");
                        return UserBehavior.of(elems[0], elems[1], elems[2], elems[3], Long.parseLong(elems[4]) * 1000L);
                    }
                })
                .filter(r -> "pv".equals(r.behavior))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );


//        Table table = tableEnv
//                .fromDataStream(
//                        stream,
//                        Schema.newBuilder()
//                                .column("userId", DataTypes.STRING())
//                                .column("itemId", "STRING")
//                                .column("categoryId", DataTypes.of(String.class))
//                                .column("behavior", DataTypes.of("STRING"))
//                                .column("timestamp", DataTypes.BIGINT())
//                                .columnByExpression("ts","CAST(timestamp AS TIMESTAMP(3))")
////                                .watermark("timestamp","SOURCE_WATERMARK()")
//                                .build()
//                );

        Table table = tableEnv
                .fromDataStream(
                        stream,
                        $("userId"),
                        $("itemId"),
                        $("categoryId"),
                        $("behavior"),
                        $("timestamp").rowtime().as("ts")
                );

        tableEnv.createTemporaryView("UserBehaviorTable", table);

        String sql = "SELECT\n"
                + "    t1.*\n"
                + "FROM\n"
                + "(\n"
                + "    SELECT\n"
                + "        *,\n"
                + "        ROW_NUMBER() OVER(PARTITION BY winEnd ORDER BY cnt DESC) AS row_num\n"
                + "    FROM\n"
                + "    (\n"
                + "        SELECT\n"
                + "            itemId,\n"
                + "            COUNT(itemId) as cnt,\n"
                + "            HOP_END(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR)  as winEnd  \n"
                + "        FROM UserBehaviorTable\n"
                + "        GROUP BY itemId, HOP(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR)\n"
                + "    )t\n"
                + ")t1\n"
                + "WHERE row_num <= 3";

        Table result = tableEnv.sqlQuery(sql);

        tableEnv.toChangelogStream(result).print();


        env.execute();


    }

    public static class UserBehavior {
        public String userId;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        public static UserBehavior of(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }

}
