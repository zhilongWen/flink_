package com.at.table.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

/**
 * @create 2022-09-01
 */
@FunctionHint(
        input = @DataTypeHint("STRING"),
        output = @DataTypeHint("STRING")
)
public class SplitUdtf extends TableFunction<String> {

    // 可选，open方法可不编写。如果编写，则需要添加声明'import org.apache.flink.table.functions.FunctionContext;'。
    @Override
    public void open(FunctionContext context) {
        // ... ...
    }

    public void eval(String str) {
        String[] split = str.split(",");
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

