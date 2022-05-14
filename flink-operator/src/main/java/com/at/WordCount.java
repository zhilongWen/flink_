package com.at;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @create 2022-05-14
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        // 获取流运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        //读取数据源 nc -lk 9099
        DataStreamSource<String> sourceStream = env.socketTextStream("127.0.0.1", 9099);


        // map操作
        SingleOutputStreamOperator<WordWithCount> mappedStream = sourceStream
                // 输入泛型：String; 输出泛型：WordWithCount
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        Arrays.stream(value.split(" ")).forEach(elem -> out.collect(new WordWithCount(elem, 1L)));
                    }
                });


        // 分组：shuffle
        KeyedStream<WordWithCount, String> keyedStream = mappedStream
                // 第一个泛型：流中元素类型
                // 第二个泛型：key 类型
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        return value.word;
                    }
                });

        // reduce操作
        // reduce会维护一个累加器
        // 第一条数据到来，作为累加器输出
        // 第二条数据到来，和累加器进行聚合操作，然后输出累加器
        // 累加器和流中元素的类型是一样的
        SingleOutputStreamOperator<WordWithCount> result = keyedStream
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });

        // 控制台输出
        result.print();

        // 执行程序
        env.execute();


    }

    public static class WordWithCount {
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }


}
