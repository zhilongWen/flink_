package com.at.keyby;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class KeyByOperator {

    // 按 user 分组

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        // keyBy：将相同 key 的 数据分发到同一逻辑分区

        env
                .addSource(new ClickSource())
                // 第一个泛型：流数据类型
                // 第二个泛型：key 类型
                .keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.user;
                    }
                })
                .print();

        env.execute();


    }

}
