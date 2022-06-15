package com.at.file;

import com.at.pojo.ItemViewCount;
import com.at.pojo.UserBehavior;
import com.at.proto.ItemViewCountProto;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
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
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.avro.AvroBuilder;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @create 2022-06-03
 */
public class FileConnection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<UserBehavior> pvSourceStream = env
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
                .filter(u -> "pv".equals(u.behavior));


        SingleOutputStreamOperator<ItemViewCountProto.ItemViewCount> result = pvSourceStream
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


//        org.apache.avro.Schema
        Schema schema = ReflectData.get().getSchema(ItemViewCount.class);

        //OutputFileConfig  https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/filesystem/#file-sink
        OutputFileConfig outputFileConfig = OutputFileConfig
                .builder()
                .withPartPrefix("prefix") // 文件前缀
                .withPartSuffix(".ext") // 文件后缀
                .build();


        // String format
        FileSink<String> sinkStringFile = FileSink
                .forRowFormat(new Path("D:\\workspace\\flink_\\files\\format"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                .withRolloverInterval(Duration.ofSeconds(10))   // 10s 滚动生成一个新文件
                                .withInactivityInterval(Duration.ofSeconds(10)) // 空闲 10s 没有来数据 滚动生成一个新文件
                                .withMaxPartSize(MemorySize.ofMebiBytes(1)) // 文件大小 超过 1M 生成一个新文件
                                .build()
                )
                .withOutputFileConfig(outputFileConfig)
                .withBucketAssigner(new MyDataTimeBucketAssigner<>())
                .enableCompact(
                        FileCompactStrategy
                                .Builder
                                .newBuilder()
                                .setNumCompactThreads(10)
                                .enableCompactionOnCheckpoint(5)
                                .build(),
                        new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(SimpleStringDecoder::new))
                )
                .build();

//        pvSourceStream
//                .map(r -> r.toString())
//                .sinkTo(sinkStringFile)
//                .uid("compact-p"); //Sink Sink requires to set a uid since its customized topology has set uid fo


        //parquet format
        /*

            <dependency>
                <groupId>org.apache.parquet</groupId>
                <artifactId>parquet-protobuf</artifactId>
                <version>1.12.2</version>
            </dependency>
            <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common -->
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-common</artifactId>
                <version>3.3.0</version>
                <scope>provided</scope>
            </dependency>
            <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs -->
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-hdfs</artifactId>
                <version>3.3.0</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-mapreduce-client-common</artifactId>
                <version>3.3.0</version>
            </dependency>

         */
        FileSink<ItemViewCountProto.ItemViewCount> sinkParquetFile = FileSink
                .forBulkFormat(new Path("D:\\workspace\\flink_\\files\\format"), ParquetProtoWriters.forType(ItemViewCountProto.ItemViewCount.class))
                .withRollingPolicy(
                        OnCheckpointRollingPolicy.build()

                )
                .withOutputFileConfig(outputFileConfig)
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .build();

//        result.sinkTo(sinkParquetFile);

        // avro format
        FileSink<ItemViewCountProto.ItemViewCount> sinkAvroFile = FileSink
                .forBulkFormat(
                        new Path("D:\\workspace\\flink_\\files\\format"),
                        new AvroWriterFactory<>(new AvroBuilder<ItemViewCountProto.ItemViewCount>() {
                            @Override
                            public DataFileWriter<ItemViewCountProto.ItemViewCount> createWriter(OutputStream outputStream) throws IOException {

                                Schema schema = ReflectData.get().getSchema(ItemViewCountProto.ItemViewCount.class);
                                ReflectDatumWriter<ItemViewCountProto.ItemViewCount> datumWriter = new ReflectDatumWriter<>(schema);

                                DataFileWriter<ItemViewCountProto.ItemViewCount> dataFileWriter = new DataFileWriter<>(datumWriter);
                                dataFileWriter.setCodec(CodecFactory.snappyCodec());
                                dataFileWriter.create(schema,outputStream);

                                return dataFileWriter;
                            }
                        })
                )
                .build();

//        result.sinkTo(sinkAvroFile).uid("avro-uid");

        // orc format

        String orcSchema = "struct<_col0:int,_col1:bigint,_col2:int,_col3:string,_col4:bigint>";
        final OrcBulkWriterFactory<UserBehavior> orcBulkWriterFactory = new OrcBulkWriterFactory<>(new ConsumerVectorizer(orcSchema));
        FileSink sinkOrcFile = FileSink
                .forBulkFormat(
                        new Path("D:\\workspace\\flink_\\files\\format"),
                        orcBulkWriterFactory
                )
                .build();

//        pvSourceStream.sinkTo(sinkOrcFile);




        env.execute("file sink test");

    }

    static class ConsumerVectorizer extends Vectorizer<UserBehavior> implements Serializable{
        public ConsumerVectorizer(String schema) {
            super(schema);
        }

        @Override
        public void vectorize(UserBehavior u, VectorizedRowBatch batch) throws IOException {

            org.apache.hadoop.hive.ql.exec.vector.LongColumnVector userIdColVector = (LongColumnVector) batch.cols[0];
            org.apache.hadoop.hive.ql.exec.vector.LongColumnVector itemIdColVector = (LongColumnVector) batch.cols[1];
            org.apache.hadoop.hive.ql.exec.vector.LongColumnVector categoryIdColVector = (LongColumnVector) batch.cols[2];
            BytesColumnVector behaviorColVector = (BytesColumnVector)batch.cols[3];
            org.apache.hadoop.hive.ql.exec.vector.LongColumnVector tsColVector = (LongColumnVector) batch.cols[4];

            int row = batch.size++;

            userIdColVector.vector[row] = u.getUserId();
            itemIdColVector.vector[row] = u.getItemId();
            categoryIdColVector.vector[row] = u.getCategoryId();
            behaviorColVector.setVal(row,u.behavior.getBytes(StandardCharsets.UTF_8));
            tsColVector.vector[row] = u.getTs();


        }
    }

    //org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
    static class MyDataTimeBucketAssigner<IN> implements BucketAssigner<IN, String> {

        private static final long serialVersionUID = 1L;
        private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH-mm";
        private final String formatString;
        private final ZoneId zoneId;
        private transient DateTimeFormatter dateTimeFormatter;

        public MyDataTimeBucketAssigner() {
            this("yyyy-MM-dd--HH-mm");
        }

        public MyDataTimeBucketAssigner(String formatString) {
            this(formatString, ZoneId.systemDefault());
        }

        public MyDataTimeBucketAssigner(ZoneId zoneId) {
            this("yyyy-MM-dd--HH-mm", zoneId);
        }

        public MyDataTimeBucketAssigner(String formatString, ZoneId zoneId) {
            this.formatString = (String) Preconditions.checkNotNull(formatString);
            this.zoneId = (ZoneId) Preconditions.checkNotNull(zoneId);
        }


        @Override
        public String getBucketId(IN in, Context context) {

            if (this.dateTimeFormatter == null) {
                this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(this.zoneId);
            }

            return this.dateTimeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime()));
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }

        @Override
        public String toString() {
            return "MyDataTimeBucketAssigner{" +
                    "formatString='" + formatString + '\'' +
                    ", zoneId=" + zoneId +
                    ", dateTimeFormatter=" + dateTimeFormatter +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyDataTimeBucketAssigner<?> that = (MyDataTimeBucketAssigner<?>) o;
            return Objects.equals(formatString, that.formatString) && Objects.equals(zoneId, that.zoneId) && Objects.equals(dateTimeFormatter, that.dateTimeFormatter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(formatString, zoneId, dateTimeFormatter);
        }
    }

    static class TopN extends KeyedProcessFunction<Long, ItemViewCount, ItemViewCountProto.ItemViewCount> {

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
        public void processElement(ItemViewCount value, Context ctx, Collector<ItemViewCountProto.ItemViewCount> out) throws Exception {
            windowAllItemData.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemViewCountProto.ItemViewCount> out) throws Exception {
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

            List<ItemViewCountProto.ItemViewCount> result = new ArrayList<>();

            for (int i = 0; i < this.N; i++) {
                ItemViewCount v = itemViewCountArrayList.get(i);
                out.collect(
                        ItemViewCountProto.ItemViewCount
                                .newBuilder()
                                .setItemId(v.getItemId())
                                .setWindowStart(v.getWindowStart())
                                .setWindowEnd(v.getWindowEnd())
                                .setCount(v.getCount())
                                .build()
                );
            }


        }
    }

}
