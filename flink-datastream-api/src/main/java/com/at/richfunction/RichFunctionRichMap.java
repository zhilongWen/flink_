package com.at.richfunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class RichFunctionRichMap {


    /*

常规函数的前面加上Rich前缀就是富函数
● RichMapFunction
● RichFlatMapFunction
● RichFilterFunction
● …
用富函数时，可以实现两个额外的方法：
● open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。open()函数通常用来做一些只需要做一次即可的初始化工作。
● close()方法是生命周期中的最后一个调用的方法，通常用来做一些清理工作。
另外，getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，当前子任务的索引，当前子任务的名字。同时还它还包含了访问分区状态的方法

    */

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
