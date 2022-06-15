package com.at.cdc;

import com.alibaba.fastjson2.JSON;
import com.at.pojo.Order;
import com.at.util.EnvironmentUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.ConnectSchema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.errors.DataException;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.header.Headers;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonSerializer;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.elasticsearch.index.engine.SafeCommitInfo;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @create 2022-06-08
 */
public class MySQLCDCConnector {


    /*

		<!-- flink cdc -->
		<dependency>
			<groupId>com.ververica</groupId>
			<artifactId>flink-sql-connector-mysql-cdc</artifactId>
			<version>2.2.1</version>
			<systemPath>${consumer.lib}/flink-sql-connector-mysql-cdc-2.2.1.jar</systemPath>
			<scope>system</scope>
		</dependency>
		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>8.0.28</version>
		</dependency>


     */

    public static void main(String[] args) throws Exception {

        //--execute.mode stream --enable.table.env true

        EnvironmentUtil.Environment environment = EnvironmentUtil.getExecutionEnvironment(args);

        StreamExecutionEnvironment env = environment.getEnv();

        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_report")
                .tableList("gmall_report.orders_tbl")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

//        MySqlSource<Order> mySqlSource = MySqlSource
//                .<Order>builder()
//                .hostname("hadoop102")
//                .port(3306)
//                .databaseList("gmall_report")
//                .tableList("gmall_report.orders_tbl")
//                .username("root")
//                .password("root")
//                .deserializer(new CustomerSchema())
//                .build();

        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MYSQL SOURCE")
                .print().setParallelism(1);


        env.execute();

//com.ververica.cdc.debezium.DebeziumDeserializationSchema
    }

    static class CustomerSchema implements DebeziumDeserializationSchema<Order> {

        private JsonNodeFactory JSON_NODE_FACTORY = null;


        public CustomerSchema() {
            this.JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
        }


        @Override
        public void deserialize(SourceRecord record, Collector<Order> collector) throws Exception {

            String topic = record.topic();
//            Schema valueSchema = record.valueSchema();
            Object value = record.value();
            Schema schema = record.valueSchema();
//            Map<String, ?> stringMap = record.sourceOffset();
//            Map<String, ?> sourcePartition = record.sourcePartition();
//            Headers headers = record.headers();
//            Integer partition = record.kafkaPartition();
//            Object key = record.key();
//            Schema keySchema = record.keySchema();
//            Long timestamp = record.timestamp();


//            System.out.println("topic：" + topic);
//            System.out.println("valueSchema：" + valueSchema);
//            System.out.println("value：" + value);
//            System.out.println("stringMap：" + stringMap);
//            System.out.println("sourcePartition：" + sourcePartition);
//            System.out.println("headers：" + headers);
//            System.out.println("partition：" + partition);
//            System.out.println("key：" + key);
//            System.out.println("keySchema：" + keySchema);
//            System.out.println("timestamp：" + timestamp);

            String[] dbAndTableName = topic.split(".");
//            System.out.println(JSON.toJSONString(value));

            System.out.println("----------------------------------------------------------------------------------------------");
            System.out.println("----------------------------------------------------------------------------------------------");
            System.out.println("----------------------------------------------------------------------------------------------");
            System.out.println("----------------------------------------------------------------------------------------------");

            JsonNode jsonNode = convertToJson(record.valueSchema(), record.value());

            System.out.println("jsonNode：" + jsonNode);


            System.out.println("============================");

        }


        private JsonNode convertToJson(Schema schema1, Object value) {

            JsonSerializer serializer = new com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonSerializer();

            Struct struct = (Struct) value;

            System.out.println(struct);

            Struct afterStruct = (Struct) struct.get("after");
            Struct beforeStruct = (Struct) struct.get("before");

            System.out.println("afterStruct：" + afterStruct);
            System.out.println("beforeStruct：" + beforeStruct);

            if (afterStruct != null) {

                ObjectNode obj = JSON_NODE_FACTORY.objectNode();

                Schema schema = afterStruct.schema();

                Iterator<Field> iterator = schema.fields().iterator();

                while (iterator.hasNext()) {
                    Field field = iterator.next();
                    Schema.Type type = field.schema().type();
                    obj.set(field.name(),getF(type,afterStruct.get(field)));
                }

                byte[] afterStructs = serializer.serialize("afterStruct", obj);

                System.out.println("oopp ==> " + new String(afterStructs));



            }


            if (beforeStruct != null) {

                Schema schema = beforeStruct.schema();

                Iterator<Field> iterator = schema.fields().iterator();

            }


            return null;


        }


        public JsonNode getF(Schema.Type schemaType, Object value) {


            switch (schemaType) {
                case BOOLEAN:
                    return JSON_NODE_FACTORY.booleanNode((Boolean) value);
                case BYTES:
                    if (value instanceof byte[]) {
                        return JSON_NODE_FACTORY.binaryNode((byte[])((byte[])value));
                    } else {
                        if (value instanceof ByteBuffer) {
                            return JSON_NODE_FACTORY.binaryNode(((ByteBuffer)value).array());
                        }

                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                    }
                case FLOAT64:
                    return JSON_NODE_FACTORY.numberNode((Double) value);
                case FLOAT32:
                    return JSON_NODE_FACTORY.numberNode((Float) value);
                case INT8:
                    return JSON_NODE_FACTORY.numberNode((Byte) value);
                case INT16:
                    return JSON_NODE_FACTORY.numberNode((Short) value);
                case INT32:
                    return JSON_NODE_FACTORY.numberNode((Integer) value);
                case INT64:
                    return JSON_NODE_FACTORY.numberNode((Long) value);
                case STRING:
                    CharSequence charSeq = (CharSequence) value;
                    return JSON_NODE_FACTORY.textNode(charSeq.toString());
                default:
                    throw new DataException("Couldn't convert " + value + " to JSON.");
            }


        }


        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }


}
