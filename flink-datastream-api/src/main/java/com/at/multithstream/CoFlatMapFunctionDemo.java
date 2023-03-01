package com.at.multithstream;

import com.at.pojo.Event;
import com.at.source.ClickSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author zero
 * @create 2023-03-01
 */
public class CoFlatMapFunctionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        /*
        nc -lk 9092
            Mary
            Alice

         */
        DataStreamSource<String> ruleStream = env.socketTextStream("hadoop102", 9092);

        // connect
        // 只能合并两条流
        // 两条流的元素的类型可以不一样
        ConnectedStreams<Event, String> connectedStreams = clickStream
                .connect(ruleStream);

        connectedStreams
                .flatMap(
                        //CoMapFunction
                        new CoFlatMapFunction<Event, String, Event>() {

                            private String rule;

                            @Override
                            public void flatMap1(Event value, Collector<Event> out) throws Exception {

                                // 按规则匹配 点击流

                                if (StringUtils.equals(rule,value.user)) out.collect(value);
                            }

                            @Override
                            public void flatMap2(String value, Collector<Event> out) throws Exception {
                                // 处理 connect 中的 规则流
                                rule = value;
                            }
                        }
                )
                .print();


        env.execute();

/*
1> Event{user='Mary', url='./cart', timestamp=2023-03-01 21:46:14.381}
1> Event{user='Mary', url='./prod?id=2', timestamp=2023-03-01 21:46:24.27}
1> Event{user='Mary', url='./prod?id=2', timestamp=2023-03-01 21:46:29.159}
1> Event{user='Mary', url='./prod?id=1', timestamp=2023-03-01 21:46:47.629}
1> Event{user='Mary', url='./prod?id=2', timestamp=2023-03-01 21:46:54.159}
1> Event{user='Mary', url='./cart', timestamp=2023-03-01 21:47:00.694}
1> Event{user='Mary', url='./prod?id=2', timestamp=2023-03-01 21:47:03.958}
2> Event{user='Alice', url='./cart', timestamp=2023-03-01 21:47:08.952}
1> Event{user='Mary', url='./prod?id=2', timestamp=2023-03-01 21:47:12.115}
2> Event{user='Alice', url='./fav', timestamp=2023-03-01 21:47:13.849}
2> Event{user='Alice', url='./home', timestamp=2023-03-01 21:47:17.104}
1> Event{user='Mary', url='./fav', timestamp=2023-03-01 21:47:18.63}
1> Event{user='Mary', url='./prod?id=1', timestamp=2023-03-01 21:47:23.528}
2> Event{user='Alice', url='./fav', timestamp=2023-03-01 21:47:28.53}
1> Event{user='Mary', url='./home', timestamp=2023-03-01 21:47:31.692}
 */
    }


}
