package com.at.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2022-05-15
 */
public class SourceFromFile {

    // 从文件读取数据

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .readTextFile("files\\1.txt")
                .print();

        env.execute();

    }


}
