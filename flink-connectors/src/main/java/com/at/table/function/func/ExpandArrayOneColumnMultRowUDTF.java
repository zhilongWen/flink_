package com.at.table.function.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

/**
 * @create 2022-09-01
 */
@FunctionHint(
        input = @DataTypeHint("ARRAY<STRING>"),
        output = @DataTypeHint("STRING")
)
public class ExpandArrayOneColumnMultRowUDTF extends TableFunction {

    public void eval(String... productImages) {

        for (String productImage : productImages) {

            collect(productImage);

        }
    }


}
