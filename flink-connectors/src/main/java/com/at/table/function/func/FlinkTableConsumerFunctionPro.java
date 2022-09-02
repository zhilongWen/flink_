package com.at.table.function.func;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-09-01
 */
public class FlinkTableConsumerFunctionPro {

    public static void main(String[] args) throws Exception {

        //https://blog.csdn.net/u010002184/article/details/125779990


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String sourceSQL = "CREATE TABLE kafka_source_table (\n"
                + "        name string,\n"
                + "        `data` ARRAY<ROW<content_type STRING,url STRING>>\n"
                + ")\n"
                + "WITH (\n"
                + "    'connector' = 'kafka', -- 使用 kafka connector\n"
                + "    'topic' = 'test-parse-json-topic',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',  -- broker连接信息\n"
                + "    'properties.group.id' = 'test-parse-json-topic-group-id', -- 消费kafka的group_id\n"
                + "    'scan.startup.mode' = 'latest-offset',  -- 读取数据的位置\n"
                + "    'format' = 'json',  -- 数据源格式为 json\n"
                + "    'json.fail-on-missing-field' = 'false', -- 字段丢失任务不失败\n"
                + "    'json.ignore-parse-errors' = 'true'  -- 解析失败跳过\n"
                + ")\n";


        tableEnv.executeSql(sourceSQL);

//        tableEnv.executeSql("select * from kafka_source_table").print();


        // CROSS JOIN unset
//        tableEnv.executeSql("select name,content_type,url\n"
//                + "from kafka_source_table CROSS JOIN UNNEST(`data`) AS t (content_type,url)").print();
/*
+----+--------------------------------+--------------------------------+--------------------------------+
| op |                           name |                   content_type |                            url |
+----+--------------------------------+--------------------------------+--------------------------------+
| +I |                       JasonLee |                          flink |                            111 |
| +I |                       JasonLee |                          spark |                            222 |
| +I |                       JasonLee |                         hadoop |                            333 |
 */



        // UNNEST
//        tableEnv.executeSql("select name,content_type,url\n"
//                + "from kafka_source_table, UNNEST(`data`) AS t (content_type,url)").print();
/*

+----+--------------------------------+--------------------------------+--------------------------------+
| op |                           name |                   content_type |                            url |
+----+--------------------------------+--------------------------------+--------------------------------+
| +I |                       JasonLee |                          flink |                            111 |
| +I |                       JasonLee |                          spark |                            222 |
| +I |                       JasonLee |                         hadoop |                            333 |

*/


//        tableEnv.executeSql("select name,content_type,url\n"
//                + "from kafka_source_table left join UNNEST(`data`) AS t (content_type,url) on true\n").print();

//
//+----+--------------------------------+--------------------------------+--------------------------------+
//| op |                           name |                   content_type |                            url |
//+----+--------------------------------+--------------------------------+--------------------------------+
//| +I |                       JasonLee |                          flink |                            111 |
//| +I |                       JasonLee |                          spark |                            222 |
//| +I |                       JasonLee |                         hadoop |                            333 |
//

        tableEnv.createTemporarySystemFunction("ParserJsonArray", ParserJsonArray.class);

        tableEnv.executeSql("select name,content_type,url\n"
                + "from kafka_source_table, lateral TABLE (ParserJsonArray(`data`)) AS t (content_type,url)\n").print();


    }

}
