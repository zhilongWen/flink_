package com.at.table.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import static org.apache.flink.table.api.Expressions.*;

/**
 * @create 2022-09-01
 */
public class ConsumerFunction {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        tableEnv.registerFunction("SPLIT", new SplitUdtf());


        // 方式1
        tableEnv.registerFunction("SubstringFunction",new SubstringFunction());



        DataStreamSource<Tuple2<String, String>> streamSource = env
                .fromCollection(
                        Arrays.asList(
                                Tuple2.of("cat_page", "channel_page,goods_page,search_page"),
                                Tuple2.of("detail_page", "search_page")
                        )
                );


        Schema schema = Schema
                .newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .build();

        tableEnv.createTemporaryView("source_tbl",streamSource, schema);

        // 方式2

//        // call function "inline" without registration in Table API
//        tableEnv.from("source_tbl").select(call(SubstringFunction.class, $("f0"), 0, 3));
//
//        // register function
//        tableEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);
//
//        // call registered function in Table API
//        tableEnv.from("source_tbl").select(call("SubstringFunction", $("f0"), 0, 3));

//        tableEnv.executeSql("select SubstringFunction(f0,0,2) from source_tbl").print();


// -----------------------------------------------------------------------


//        tableEnv
//                .from("source_tbl")
//                .joinLateral(call(SplitUdtf.class,$("f1")))
//                .select($("f1"));

        // register function
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitUdtf.class);


        tableEnv.executeSql("select f0,info from source_tbl,LATERAL TABLE(SplitFunction(f1)) AS T(info) ").print();


        env.execute();


    }



}
