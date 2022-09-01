package com.at.table.function.func;

import com.at.table.function.SplitUdtf;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-09-01
 */
public class FlinkTableConsumerFunction {

    public static void main(String[] args) throws Exception {

        //https://blog.csdn.net/u010002184/article/details/125779990


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String sourceSQL = "CREATE TABLE source_table (\n"
                + "    userId INT,\n"
                + "    eventTime as '2021-10-01 08:08:08',\n"
                + "    eventType as 'click',\n"
                + "    productId INT,\n"
                + "    -- 数组(Array)类型\n"
                + "    productImages as ARRAY['image1','image2'],\n"
                + "    -- 对象(Map)类型\n"
                + "    pageInfo as MAP['pageId','1','pageName','yyds']\n"
                + ") WITH (\n"
                + "    'connector' = 'datagen',\n"
                + "    'number-of-rows' = '2',\n"
                + "    'fields.userId.kind' = 'random',\n"
                + "    'fields.userId.min' = '2',\n"
                + "    'fields.userId.max' = '2',\n"
                + "    'fields.productId.kind' = 'sequence',\n"
                + "    'fields.productId.start' = '1',\n"
                + "    'fields.productId.end' = '2'\n"
                + ")\n";


        tableEnv.executeSql(sourceSQL);


        // -------------------------------------------------------------------------------------
        // 将Map展开为多列多行
        // -------------------------------------------------------------------------------------

        // unset
//        tableEnv.executeSql("SELECT userId,eventTime,eventType,productId,mapKey,mapValue \n"
//                + "FROM source_table, UNNEST(pageInfo) as t(mapKey,mapValue)").print();

        // register function
//        tableEnv.createTemporarySystemFunction("ExpandMapMultColumnMultRowUDTF", ExpandMapMultColumnMultRowUDTF.class);
//        tableEnv.executeSql("SELECT \n"
//                + "        userId,\n"
//                + "        eventTime,\n"
//                + "        eventType,\n"
//                + "        productId,\n"
//                + "        mapKey,\n"
//                + "        mapValue \n"
//                + "FROM source_table, \n"
//                + "        LATERAL TABLE (ExpandMapMultColumnMultRowUDTF(`pageInfo`)) AS t(mapKey,mapValue)").print();

/*
        +-------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+
                |      userId |                      eventTime |                      eventType |   productId |                         mapKey |                       mapValue |
                +-------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+
                |           2 |            2021-10-01 08:08:08 |                          click |           1 |                         pageId |                              1 |
|           2 |            2021-10-01 08:08:08 |                          click |           1 |                       pageName |                           yyds |
|           2 |            2021-10-01 08:08:08 |                          click |           2 |                         pageId |                              1 |
|           2 |            2021-10-01 08:08:08 |                          click |           2 |                       pageName |                           yyds |
                +-------------+--------------------------------+--------------------------------+-------------+--------------------------------+--------------------------------+

*/

        // -------------------------------------------------------------------------------------
        // 将数组展开为单列多行
        // -------------------------------------------------------------------------------------

        // unset
//        tableEnv.executeSql("SELECT userId,eventTime,eventType,productId,productImage \n"
//                + "FROM source_table, UNNEST(productImages) as t(productImage)").print();

        // register function
        tableEnv.createTemporarySystemFunction("ExpandArrayOneColumnMultRowUDTF", ExpandArrayOneColumnMultRowUDTF.class);
        tableEnv.executeSql("SELECT \n"
                + "        userId, \n"
                + "        eventTime, \n"
                + "        eventType, \n"
                + "        productId, \n"
                + "        productImage \n"
                + "FROM source_table, \n"
                + "        LATERAL TABLE (ExpandArrayOneColumnMultRowUDTF(`productImages`)) AS T(productImage)").print();

/*
+-------------+--------------------------------+--------------------------------+-------------+--------------------------------+
|      userId |                      eventTime |                      eventType |   productId |                   productImage |
+-------------+--------------------------------+--------------------------------+-------------+--------------------------------+
|           2 |            2021-10-01 08:08:08 |                          click |           1 |                         image1 |
|           2 |            2021-10-01 08:08:08 |                          click |           1 |                         image2 |
|           2 |            2021-10-01 08:08:08 |                          click |           2 |                         image1 |
|           2 |            2021-10-01 08:08:08 |                          click |           2 |                         image2 |
+-------------+--------------------------------+--------------------------------+-------------+--------------------------------+

*/







    }

}
