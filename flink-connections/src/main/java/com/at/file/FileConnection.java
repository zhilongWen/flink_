package com.at.file;

import com.at.pojo.ItemViewCount;
import com.at.pojo.UserBehavior;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @create 2022-06-03
 */
public class FileConnection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<ItemViewCount> result = env
                .readTextFile("D:\\workspace\\flink_\\files\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] elems = value.split(",");
                        return UserBehavior.builder()
                                .userId(Integer.parseInt(elems[0]))
                                .itemId(Long.parseLong(elems[1]))
                                .categoryId(Integer.parseInt(elems[2]))
                                .behavior(elems[3])
                                .ts(Long.parseLong(elems[4]))
                                .build();
                    }
                })
                .filter(u -> "pv".equals(u.behavior))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.ts * 1000L;
                                            }
                                        }
                                )
                )
                .keyBy(u -> u.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        },
                        new ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow>() {
                            @Override
                            public void process(Long key, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {

                                long windowStart = context.window().getStart();
                                long windowEnd = context.window().getEnd();

                                Long count = elements.iterator().next();

                                out.collect(ItemViewCount.builder().itemId(key).count(count).windowStart(windowStart).windowEnd(windowEnd).build());

                            }
                        }
                )
                .keyBy(r -> r.getWindowEnd())
                .process(new TopN(3));


        //String
        FileSink<String> sinkStringFile = FileSink
                .forRowFormat(new Path(""), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                .withRolloverInterval(Duration.ofSeconds(10))   // 10s 滚动生成一个新文件
                                .withInactivityInterval(Duration.ofSeconds(10)) // 空闲 10s 没有来数据 滚动生成一个新文件
                                .withMaxPartSize(MemorySize.ofMebiBytes(1)) // 文件大小 超过 1M 生成一个新文件
                                .build()
                )
//                .withOutputFileConfig()
                .build();



//        FileSink<Object> sinkAvroFile = FileSink
//                .forBulkFormat()
//                .build();


        env.execute();

    }

    static class TopN extends KeyedProcessFunction<Long, ItemViewCount, ItemViewCount> {

        private int N;

        private ListState<ItemViewCount> windowAllItemData;


        public TopN(int n) {
            N = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            windowAllItemData = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("window-all-item-count", Types.POJO(ItemViewCount.class))
            );
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<ItemViewCount> out) throws Exception {
            windowAllItemData.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemViewCount> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            ArrayList<ItemViewCount> itemViewCountArrayList = new ArrayList<>();

            for (ItemViewCount itemViewCount : windowAllItemData.get()) {
                itemViewCountArrayList.add(itemViewCount);
            }

            windowAllItemData.clear();

            itemViewCountArrayList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            List<ItemViewCount> result = new ArrayList<>();

            for (int i = 0; i < this.N; i++) {
                out.collect(itemViewCountArrayList.get(i));
            }


        }
    }

}
