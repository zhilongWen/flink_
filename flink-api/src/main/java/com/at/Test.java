//package com.at;
//
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
//import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import com.at.pojo.Event;
//import com.at.source.ClickSource;
//import io.debezium.data.Envelope;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Field;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;
//
//import java.util.Properties;
//
///**
// * @create 2022-05-17
// */
//public class Test {
//
//    public static void main(String[] args) throws Exception{
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
////        env.setStateBackend(new FsStateBackend("file://D:\\workspace\\flink_\\flink-api\\files\\sf"));
//        env.setRestartStrategy(RestartStrategies.noRestart());
//
//        Properties props = new Properties();
//        props.setProperty("debezium.snapshot.mode","initial");
//
//        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("hadoop102")
//                .port(3306)
//                .databaseList("table_process")// monitor all tables under inventory database
//                .tableList("table_process.test_flinkcdc")
//                .username("root")
//                .password("root")
//                .debeziumProperties(props)
//                .deserializer(new MySchema()) // converts SourceRecord to String
//                .build();
//
//        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
//
//        streamSource.print();
//
//        env.execute();
//
//
//    }
//
//    public static class MySchema implements DebeziumDeserializationSchema<String> {
//
//        @Override
//        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//
//            //获取主题信息,包含着数据库和表名 mysql_binlog_source.gmall-flink-200821.z_user_info
//            String topic = sourceRecord.topic();
//            String[] arr = topic.split("\\.");
//            String db = arr[1];
//            String tableName = arr[2];
//
//            //获取操作类型 READ DELETE UPDATE CREATE
//            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//
//            //获取值信息并转换为Struct类型
//            Struct value = (Struct) sourceRecord.value();
//
//            //获取变化后的数据
//            Struct after = value.getStruct("after");
//
//            //创建JSON对象用于存储数据信息
//            JSONObject data = new JSONObject();
//            if(after != null){
//                for (Field field : after.schema().fields()) {
//                    Object o = after.get(field);
//                    data.put(field.name(), o);
//                }
//            }
//
//
//            //创建JSON对象用于封装最终返回值数据信息
//            JSONObject result = new JSONObject();
//            result.put("operation", operation.toString().toLowerCase());
//            result.put("data", data);
//            result.put("database", db);
//            result.put("table", tableName);
//
//            //发送数据至下游
//            collector.collect(result.toJSONString());
//
//
//        }
//
//        @Override
//        public TypeInformation<String> getProducedType() {
//            return TypeInformation.of(String.class);
//        }
//    }
//
//}
