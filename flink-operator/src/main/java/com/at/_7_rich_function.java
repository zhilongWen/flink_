package com.at;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-14
 */
public class _7_rich_function {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .fromElements(1, 2, 3, 4)
                .map(new RichMapFunction<Integer, Integer>() {

                    // 针对每一个任务槽开启生命周期

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("生命周期开始，当前子任务的索引是：" + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("生命周期结束");
                    }
                })
                .print();


        env.execute();

    }

}
