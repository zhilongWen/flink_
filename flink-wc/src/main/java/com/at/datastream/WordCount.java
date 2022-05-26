package com.at.datastream;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * @create 2022-05-25
 */
public class WordCount {

    public static final String[] WORDS =
            new String[]{
                    "To be, or not to be,--that is the question:--",
                    "Whether 'tis nobler in the mind to suffer",
                    "The slings and arrows of outrageous fortune",
                    "Or to take arms against a sea of troubles,",
                    "And by opposing end them?--To die,--to sleep,--",
                    "No more; and by a sleep to say we end",
                    "The heartache, and the thousand natural shocks",
                    "That flesh is heir to,--'tis a consummation",
                    "Devoutly to be wish'd. To die,--to sleep;--",
                    "To sleep! perchance to dream:--ay, there's the rub;",
                    "For in that sleep of death what dreams may come,",
                    "When we have shuffled off this mortal coil,",
                    "Must give us pause: there's the respect",
                    "That makes calamity of so long life;",
                    "For who would bear the whips and scorns of time,",
                    "The oppressor's wrong, the proud man's contumely,",
                    "The pangs of despis'd love, the law's delay,",
                    "The insolence of office, and the spurns",
                    "That patient merit of the unworthy takes,",
                    "When he himself might his quietus make",
                    "With a bare bodkin? who would these fardels bear,",
                    "To grunt and sweat under a weary life,",
                    "But that the dread of something after death,--",
                    "The undiscover'd country, from whose bourn",
                    "No traveller returns,--puzzles the will,",
                    "And makes us rather bear those ills we have",
                    "Than fly to others that we know not of?",
                    "Thus conscience does make cowards of us all;",
                    "And thus the native hue of resolution",
                    "Is sicklied o'er with the pale cast of thought;",
                    "And enterprises of great pith and moment,",
                    "With this regard, their currents turn awry,",
                    "And lose the name of action.--Soft you now!",
                    "The fair Ophelia!--Nymph, in thy orisons",
                    "Be all my sins remember'd."
            };


    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        // config flink-conf.yaml info
        configuration.setString("jobmanager.memory.process.size", String.valueOf(1024 * 4));
        configuration.setString("rest.port", String.valueOf(8082));



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointStorage(new FsStateBackend("file:///D:\\workspace\\flink_\\flink-wc\\ckpt"));
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1 * 60 * 1000L);
        checkpointConfig.setCheckpointTimeout(1 * 60 * 1000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(1 * 60 * 1000L);
        checkpointConfig.enableUnalignedCheckpoints(true);
        checkpointConfig.setTolerableCheckpointFailureNumber(2);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);


        DataStreamSource<String> sourceStream = env.fromElements(WORDS);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = sourceStream
                .flatMap(
                        new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

                            private Counter inputCounter;

                            private Counter outCounter;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                inputCounter = getRuntimeContext()
                                        .getMetricGroup()
                                        .counter("inputCounter");
                                outCounter = getRuntimeContext()
                                        .getMetricGroup()
                                        .counter("inputCounter");
                            }

                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                                inputCounter.inc();

                                String[] elems = value.toLowerCase(Locale.ROOT).split("\\W+");

                                for (String elem : elems) {
                                    out.collect(Tuple2.of(elem, 1));
                                    outCounter.inc();
                                }

                            }
                        }
                );

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMapStream
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,  value1.f1 + value2.f1);
                    }
                });

        result.print();

        env.execute();


    }

}
