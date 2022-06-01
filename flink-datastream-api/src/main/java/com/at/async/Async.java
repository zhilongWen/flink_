package com.at.async;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @create 2022-05-31
 */
public class Async {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);


        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());


        SingleOutputStreamOperator<ClickUserInfo> result = AsyncDataStream
                .unorderedWait(
                        clickStream,
                        new AsyncConnectionFunction() {
                            @Override
                            public String getKey(Event event) {
                                return event.user;
                            }

                            @Override
                            public ClickUserInfo join(Event input, UserInfo userInfo) {


                                if (userInfo == null) {
                                    return ClickUserInfo.of(input.user, input.url, input.timestamp, 1, 1, "1");
                                }

                                return ClickUserInfo.of(input.user, input.url, input.timestamp, userInfo.getAge(), userInfo.getSex(), userInfo.getAddress());
                            }
                        },
                        60,
                        TimeUnit.SECONDS
                );

        result.print();


        env.execute();

    }

}
