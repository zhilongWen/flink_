package com.at.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @create 2022-05-22
 */
public class CEPLibrary {

    /*

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-cep</artifactId>
            <version>1.15.0</version>
        </dependency>
     */


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        KeyedStream<Tuple3<String, String, Long>, String> keyedStream = env
                .fromElements(
                        Tuple3.of("user-1", "fail", 1 * 1000L),
                        Tuple3.of("user-1", "fail", 2 * 1000L),
                        Tuple3.of("user-2", "fail", 3 * 1000L),
                        Tuple3.of("user-1", "fail", 3 * 1000L),
                        Tuple3.of("user-1", "fail", 5 * 1000L),
                        Tuple3.of("user-2", "success", 4 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                                return element.f2;
                                            }
                                        }
                                )

                )
                .keyBy(r -> r.f0);


//        // 定义模板
//        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
//                .<Tuple3<String, String, Long>>begin("first")
//                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
//                    @Override
//                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
//                        return "fail".equals(value.f1);
//                    }
//                })
//                .next("second")
//                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
//                    @Override
//                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
//                        return "fail".equals(value.f1);
//                    }
//                })
//                .next("third")
//                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
//                    @Override
//                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
//                        return "fail".equals(value.f1);
//                    }
//                });
//
//
//        // 在流上匹配模板
//        PatternStream<Tuple3<String, String, Long>> patternStream = CEP.pattern(keyedStream, pattern);
//
//        // 使用select方法将匹配到的事件取出
//        patternStream
//                .select(
//                        new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
//                            @Override
//                            public String select(Map<String, List<Tuple3<String, String, Long>>> map) throws Exception {
//                                // Map的key是给事件起的名字
//                                // 列表是名字对应的事件所构成的列表
//
//                                Tuple3<String, String, Long> first = map.get("first").get(0);
//                                Tuple3<String, String, Long> second = map.get("second").get(0);
//                                Tuple3<String, String, Long> third = map.get("third").get(0);
//
//                                String result = "用户：" + first.f0 + " 在时间：" + first.f2 + ";" + second.f2 + ";" +
//                                        "" + third.f2 + " 登录失败了！";
//
//                                return result;
//                            }
//                        }
//                )
//                .print();


        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
                .<Tuple3<String, String, Long>>begin("fail")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return "fail".equals(value.f1);
                    }
                })
                .times(3);

        PatternStream<Tuple3<String, String, Long>> patternStream = CEP.pattern(keyedStream, pattern);

        patternStream
                .flatSelect(new PatternFlatSelectFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public void flatSelect(Map<String, List<Tuple3<String, String, Long>>> map, Collector<String> collector) throws Exception {
                        Tuple3<String, String, Long> first = map.get("fail").get(0);
                        Tuple3<String, String, Long> second = map.get("fail").get(1);
                        Tuple3<String, String, Long> third = map.get("fail").get(2);
                        String result = "用户：" + first.f0 + " 在时间：" + first.f2 + ";" + second.f2 + ";" +
                                "" + third.f2 + " 登录失败了！";
                        collector.collect(result);
                    }
                })
                .print();

//        patternStream
//                .select(
//                        new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
//                            @Override
//                            public String select(Map<String, List<Tuple3<String, String, Long>>> map) throws Exception {
//                                Tuple3<String, String, Long> first = map.get("fail").get(0);
//                                Tuple3<String, String, Long> second = map.get("fail").get(1);
//                                Tuple3<String, String, Long> third = map.get("fail").get(2);
//                                String result = "用户：" + first.f0 + " 在时间：" + first.f2 + ";" + second.f2 + ";" +
//                                        "" + third.f2 + " 登录失败了！";
//                                return result;
//                            }
//                        }
//                )
//                .print();

        env.execute();


    }

}
