package com.at.conntctors.kafka;

import com.alibaba.fastjson2.JSON;
import com.at.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * @create 2022-06-01
 */
public class KafkaConnection {

    /*
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-kafka</artifactId>
			<version>${flink.version}</version>
		</dependency>
     */

    public static void main(String[] args) throws Exception {


        // 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics(Arrays.asList("user_behaviors"))
                .setGroupId("test-group-id")
                .setStartingOffsets(OffsetsInitializer.timestamp(1654089523897L))
//                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000") // discover new partitions per 10 seconds
                .build();

        // 每 15s 输出一次 最近 1min 每个行为前 3 的用户信息
        env
                .fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source-operator"
                )
                .map(r -> JSON.parseObject(r, UserBehavior.class))
                .returns(Types.POJO(UserBehavior.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.ts;
                                            }
                                        }
                                )
                )
                .keyBy(u -> u.behavior)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(15)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Map<String, Integer>, Map<String, Integer>>() {
                            @Override
                            public Map<String, Integer> createAccumulator() {
                                return new HashMap<String, Integer>();
                            }

                            @Override
                            public Map<String, Integer> add(UserBehavior value, Map<String, Integer> accumulator) {
                                accumulator.put(String.valueOf(value.userId), accumulator.getOrDefault(String.valueOf(value.userId), 0) + 1);
                                return accumulator;
                            }

                            @Override
                            public Map<String, Integer> getResult(Map<String, Integer> accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {

                                a.forEach(new BiConsumer<String, Integer>() {
                                    @Override
                                    public void accept(String ka, Integer va) {
                                        b.merge(ka, va, new BiFunction<Integer, Integer, Integer>() {
                                            @Override
                                            public Integer apply(Integer value_a, Integer value_b) {
                                                return value_a + value_b;
                                            }
                                        });
                                    }
                                });

                                return b;
                            }
                        },
                        new ProcessWindowFunction<Map<String, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Map<String, Integer>> elements, Collector<String> out) throws Exception {


                                Map<String, Integer> map = elements.iterator().next();

                                System.out.println("key：" + key + "\tmap size : " + map.size());

                                PriorityQueue<Tuple2<String, Integer>> priorityQueue = new PriorityQueue<>(map.size(), new Comparator<Tuple2<String, Integer>>() {
                                    @Override
                                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                                        return o2.f1.intValue() - o1.f1.intValue();
                                    }
                                });

                                map.forEach((k, v) -> priorityQueue.add(Tuple2.of(k, v)));

                                Timestamp winS = new Timestamp(context.window().getStart());
                                Timestamp winE = new Timestamp(context.window().getEnd());

                                StringBuffer result = new StringBuffer();

                                result
                                        .append("======================================\n")
                                        .append("\t--- behavior：")
                                        .append(key)
                                        .append(" ---\n")
                                        .append("窗口 [ ")
                                        .append(winS)
                                        .append(" - ")
                                        .append(winE)
                                        .append(" )")
                                        .append("\n");

                                int topN = map.size() > 3 ? 3 : map.size();

                                for (int i = 0; i < topN; i++) {

                                    Tuple2<String, Integer> top = priorityQueue.poll();

                                    result
                                            .append("第 ")
                                            .append(i + 1)
                                            .append(" 名的行为用户：")
                                            .append(top.f0)
                                            .append(" 浏览量：")
                                            .append(top.f1)
                                            .append("\n");
                                }

                                result.append("======================================\n\n\n");

                                out.collect(result.toString());

                            }
                        }
                )
                .print();


        env.execute();

    }

}
