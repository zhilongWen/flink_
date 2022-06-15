package com.at.table.function;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @create 2022-06-10
 */
public class FunctionSQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


//        tableEnv.executeSql("select char_length('wer') var1").print();

//        tableEnv.executeSql("select chr(97)").print();

//        tableEnv.executeSql("select concat('hello', 'world') var1").print();

//        tableEnv.executeSql("select concat_ws('|', 'bigdata', 'flink') var1").print();

//        tableEnv.executeSql("select from_base64('') var1, from_base64('SGVsbG8gd29ybGQ=') var2").print();

//        tableEnv.executeSql("select hash_code('') var1, hash_code('oop') var2").print();

//        tableEnv.executeSql("select initcap('aBAb') var1").print();

//        tableEnv.executeSql("select instr('hello word', 'lo') var1, instr('hello word', 'l', -1, 1) var2, instr('hello word', 'l', 3, 2) var3").print();

//        tableEnv.executeSql("select lower('TTy')").print();

//        tableEnv.executeSql("select md5('TTy') var1, md5('') var2").print();

//        tableEnv.executeSql("select overlay('abcdefg' placing 'hij' from 2 for 2) var1").print();

//        tableEnv.executeSql("select position('in' in 'china') var1").print();

//        tableEnv.executeSql("select regexp_extract('foothebar','foo(.*?)(bar)',2) var1").print();

//        tableEnv.executeSql("select repeat('in',3) var1,repeat('in',-1) var1").print();

//        tableEnv.executeSql("select replace('alibaba flink','alibaba','Apache') var1").print();

//        tableEnv.executeSql("select reverse('flink') var1,reverse('null') var2").print();

//        tableEnv.executeSql("select rpad('hello',5,'flink') var1,rpad('hello',0,'flink') var2,rpad('hello',10,'flink') var3,rpad('hello',8,'flink') var4").print();

//        tableEnv.executeSql("select str_to_map('k1=v1,k2=v2')['k1'] var1").print();


        tableEnv.executeSql("select date_format('2017-09-15 07:29:00','HH')").print();




    }

}
