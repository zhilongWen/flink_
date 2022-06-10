package com.at.table.function;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @create 2022-06-10
 */
public class ParseUdtf extends TableFunction<String> {

    // 可选，open方法可不编写。如果编写，则需要添加声明'import org.apache.flink.table.functions.FunctionContext;'。
    @Override
    public void open(FunctionContext context) {
        // ... ...
    }

    public void eval(String str) {
        String[] split = str.split("\\|");
        for (String s : split) {
            collect(s);
        }
    }

    // 可选，close方法可不编写。
    @Override
    public void close() {
        // ... ...
    }
}