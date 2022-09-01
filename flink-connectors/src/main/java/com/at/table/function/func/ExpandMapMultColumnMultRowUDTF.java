package com.at.table.function.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * @create 2022-09-01
 */
@FunctionHint(
        input = @DataTypeHint("MAP<STRING, STRING>"),
        output = @DataTypeHint("ROW<mapKey STRING, mapValue STRING>"))
public class ExpandMapMultColumnMultRowUDTF extends TableFunction {

    public void eval(Map<String, String> pageInfo) {
        for (Map.Entry<String, String> entry : pageInfo.entrySet()) {
            // 原来的一行，每个Key都输出一行
            collect(Row.of(entry.getKey(), entry.getValue()));

        }
    }
}

