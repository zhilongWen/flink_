package com.at.operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-14
 */
public class _6_shuffle {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // shuffle 方法将数据随机的分配到下游算子的并行任务中去
        // private Random random = new Random();
        // public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {return random.nextInt(numberOfChannels); }
        env
                .fromElements(1, 2, 3, 4).setParallelism(1)
                .shuffle()
                .print("shuffle：").setParallelism(2);


        // rebalance 方法使用 Round-Robin 负载均衡算法将输入流平均分配到随后的并行运行的任务中去
        // nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        env
                .fromElements(1, 2, 3, 4).setParallelism(1)
                .rebalance()
                .print("rebalance：").setParallelism(2);

        // rescale 方法使用的也是 round-robin 算法，但只会将数据发送到接下来的并行运行的任务中的一部分任务中
        env
                .fromElements(1, 2, 3, 4).setParallelism(1)
                .rescale()
                .print("rescale：").setParallelism(2);


        // broadcast 方法将输入流的所有数据复制并发送到下游算子的所有并行任务中去
        env
                .fromElements(1, 2, 3, 4).setParallelism(1)
                .broadcast()
                .print("broadcast：").setParallelism(2);


        // global 方法将所有的输入流数据都发送到下游算子的第一个并行任务中去


        env.execute();

    }

}
