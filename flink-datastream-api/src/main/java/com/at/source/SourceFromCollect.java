package com.at.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @create 2022-05-15
 */
public class SourceFromCollect {

    // source from collect

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .fromCollection(
                        Arrays.asList("tom","jock","alick")
                );

        env.fromElements("tom","jock","alick");


        env.execute();

    }

}
