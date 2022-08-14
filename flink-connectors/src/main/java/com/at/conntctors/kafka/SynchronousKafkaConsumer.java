//package com.at.conntctors.kafka;
//
//import com.google.common.util.concurrent.RateLimiter;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.metrics.MetricGroup;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
//import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
//import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
//import org.apache.flink.streaming.connectors.kafka.internals.*;
//import org.apache.flink.util.SerializedValue;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.regex.Pattern;
//
///**
// * @create 2022-08-14
// */
//public class SynchronousKafkaConsumer<T> extends FlinkKafkaConsumer<T> {
//
//
//      //https://www.saoniuhuo.com/question/detail-1911743.html
//      //https://lists.apache.org/thread/j7kw131jn0ozmrj763j0hr87b1rj7jop
//
//
//    protected static final Logger LOG = LoggerFactory.getLogger(SynchronousKafkaConsumer.class);
//
//    private final double topicRateLimit;
//    private transient RateLimiter subtaskRateLimiter;
//
//    @Override
//    public void open(Configuration configuration) throws Exception {
//        Preconditions.checkArgument(
//                topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks() > 0.1,
//                "subtask ratelimit should be greater than 0.1 QPS");
//        subtaskRateLimiter = RateLimiter.create(
//                topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks());
//        super.open(configuration);
//    }
//
//    @Override
//    protected AbstractFetcher<T, ?> createFetcher(
//            SourceContext<T> sourceContext,
//            Map<KafkaTopicPartition, Long> partitionsWithOffsets,
//            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
//            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
//            StreamingRuntimeContext runtimeContext,
//            OffsetCommitMode offsetCommitMode,
//            MetricGroup consumerMetricGroup, boolean useMetrics)
//            throws Exception {
//
//        return new KafkaFetcher<T>(
//                sourceContext,
//                partitionsWithOffsets,
//                watermarksPeriodic,
//                watermarksPunctuated,
//                runtimeContext.getProcessingTimeService(),
//                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
//                runtimeContext.getUserCodeClassLoader(),
//                runtimeContext.getTaskNameWithSubtasks(),
//                deserializer,
//                properties,
//                pollTimeout,
//                runtimeContext.getMetricGroup(),
//                consumerMetricGroup,
//                useMetrics) {
//            @Override
//            protected void emitRecord(T record,
//                                      KafkaTopicPartitionState<TopicPartition> partitionState,
//                                      long offset) throws Exception {
//                subtaskRateLimiter.acquire();
//                if (record == null) {
//                    consumerMetricGroup.counter("invalidRecord").inc();
//                }
//                super.emitRecord(record, partitionState, offset);
//            }
//
//            @Override
//            protected void emitRecordWithTimestamp(T record,
//                                                   KafkaTopicPartitionState<TopicPartition> partitionState,
//                                                   long offset, long timestamp) throws Exception {
//                subtaskRateLimiter.acquire();
//                if (record == null) {
//                    consumerMetricGroup.counter("invalidRecord").inc();
//                }
//                super.emitRecordWithTimestamp(record, partitionState, offset, timestamp);
//            }
//        };
//
//    }
//}