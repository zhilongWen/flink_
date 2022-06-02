package com.at.file;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.ExecutionException;

/**
 * @create 2022-06-02
 */
public class FileConnection {


    /*

		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-files</artifactId>
			<version>${flink.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-csv</artifactId>
			<version>${flink.version}</version>
		</dependency>

     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String sourceSQL = "CREATE TABLE file_source_tbl(\n"
                + "    user_id BIGINT,\n"
                + "    item_id BIGINT,\n"
                + "    category_id BIGINT,\n"
                + "    behavior STRING,\n"
                + "    ts BIGINT,\n"
                + "    row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss')),\n"
                + "    watermark for row_time as row_time - INTERVAL '1' SECOND \n"
                + ") WITH (\n"
                + "    'connector' = 'filesystem',   \n"
                + "    'path' = 'file:///D:\\workspace\\flink_\\files\\UserBehavior.csv',\n"
                + "    'format' = 'csv',\n"
                + "    'csv.ignore-parse-errors' = 'true',\n"
                + "    'csv.allow-comments' = 'true'\n"
                + ")";


        tableEnv.executeSql(sourceSQL);


        Table pvTable = tableEnv.sqlQuery("select * from file_source_tbl where behavior='pv'");

        tableEnv.createTemporaryView("pv_view",pvTable);

//        tableEnv.executeSql("SELECT\n"
//                + "    *\n"
//                + "FROM \n"
//                + "(\n"
//                + "    SELECT\n"
//                + "        *,\n"
//                + "        ROW_NUMBER() OVER(PARTITION BY win_end ORDER BY item_count DESC) AS row_num\n"
//                + "    FROM \n"
//                + "    (\n"
//                + "        SELECT\n"
//                + "            item_id,\n"
//                + "            COUNT(item_id) AS item_count,\n"
//                + "            HOP_START(row_time,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) as win_start,\n"
//                + "            HOP_END(row_time,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) as win_end\n"
//                + "        FROM pv_view\n"
//                + "        GROUP BY item_id,HOP(row_time,INTERVAL '5' MINUTE,INTERVAL '1' HOUR)\n"
//                + "    )t\n"
//                + ")t1 where row_num <= 3")
//                .print();

//        tableEnv.executeSql("SELECT\n"
//                + "    *\n"
//                + "FROM\n"
//                + "TABLE\n"
//                + "(\n"
//                + "    HOP(\n"
//                + "        TABLE file_source_tbl,\n"
//                + "        DESCRIPTOR(row_time),\n"
//                + "        INTERVAL '5' MINUTE,\n"
//                + "        INTERVAL '1' HOUR\n"
//                + "    )\n"
//                + ")")
//                .print();
//
//        tableEnv.executeSql("SELECT\n"
//                + "    *\n"
//                + "FROM \n"
//                + "(\n"
//                + "    SELECT\n"
//                + "        *,\n"
//                + "        ROW_NUMBER() OVER(PARTITION BY window_end ORDER BY item_count DESC) AS row_num\n"
//                + "    FROM \n"
//                + "    (\n"
//                + "        SELECT\n"
//                + "            item_id,\n"
//                + "            COUNT(item_id) as item_count,\n"
//                + "            window_start,\n"
//                + "            window_end\n"
//                + "        FROM \n"
//                + "        TABLE(\n"
//                + "            HOP(\n"
//                + "                TABLE pv_view,\n"
//                + "                DESCRIPTOR(row_time),\n"
//                + "                INTERVAL '5' MINUTE,\n"
//                + "                INTERVAL '1' HOUR\n"
//                + "            )\n"
//                + "        )\n"
//                + "        GROUP BY item_id,window_start,window_end\n"
//                + "    )t\n"
//                + ")t1\n"
//                + "WHERE row_num <=3 ")
//                .print();


//        tableEnv.executeSql("SELECT UNIX_TIMESTAMP('2017-11-26 09:05:00.000','yyyy-MM-dd HH:mm:ss.S')").print();

        String sinkSQL = "CREATE TABLE IF NOT EXISTS kafka_upset_sink_tbl(\n"
                + "    item_id BIGINT,\n"
                + "    item_count INT,\n"
                + "    window_start BIGINT,\n"
                + "    window_end BIGINT,\n"
                + "    row_num INT,\n"
                + "    PRIMARY KEY (item_id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "    'connector' = 'upsert-kafka',\n"
                + "    'topic' = 'topN_topic',\n"
                + "    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n"
                + "    'key.format' = 'json',\n"
                + "    'value.format' = 'json'\n"
                + ")\n";

        String insertSQL = "INSERT INTO kafka_upset_sink_tbl\n"
                + "SELECT\n"
                + "    CAST(item_id AS BIGINT) AS item_id,\n"
                + "    CAST(item_count AS INT) AS item_count,\n"
                + "    UNIX_TIMESTAMP(CAST(window_start AS STRING),'yyyy-MM-dd HH:mm:ss.S')  AS window_start,\n"
                + "    UNIX_TIMESTAMP(CAST(window_end AS STRING),'yyyy-MM-dd HH:mm:ss.S') AS window_end,\n"
                + "    CAST(row_num AS INT) AS row_num\n"
                + "FROM \n"
                + "(\n"
                + "    SELECT\n"
                + "        *,\n"
                + "        ROW_NUMBER() OVER(PARTITION BY window_end ORDER BY item_count DESC) AS row_num\n"
                + "    FROM \n"
                + "    (\n"
                + "        SELECT\n"
                + "            item_id,\n"
                + "            COUNT(item_id) as item_count,\n"
                + "            window_start,\n"
                + "            window_end\n"
                + "        FROM \n"
                + "        TABLE(\n"
                + "            HOP(\n"
                + "                TABLE pv_view,\n"
                + "                DESCRIPTOR(row_time),\n"
                + "                INTERVAL '5' MINUTE,\n"
                + "                INTERVAL '1' HOUR\n"
                + "            )\n"
                + "        )\n"
                + "        GROUP BY item_id,window_start,window_end\n"
                + "    )t\n"
                + ")t1\n"
                + "WHERE row_num <=3\n";

        tableEnv.executeSql(sinkSQL);
        tableEnv.executeSql(insertSQL);

        env.execute();

    }

}
