package com.at.table.function.func;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.typeutils.RowTypeUtils;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.*;
import java.util.logging.Logger;

/**
 * @create 2022-09-01
 */
@FunctionHint(
        input = @DataTypeHint("ARRAY<ROW<`content_type` STRING, `url` STRING>>"),
        output = @DataTypeHint("ROW<`content_type` STRING, `url` STRING> ")
)
public class ParserJsonArray extends TableFunction<Row> {


//    public void eval(String value) {
//
//        try {
//
//            JSONArray snapshots = JSONArray.parseArray(value);
//            Iterator<Object> iterator = snapshots.iterator();
//            while (iterator.hasNext()) {
//                JSONObject jsonObject = (JSONObject) iterator.next();
//                String content_type = jsonObject.getString("content_type");
//                String url = jsonObject.getString("url");
//
//                collect(Row.of(content_type, url));
//            }
//
//        } catch (Exception e) {
//        }
//    }


    public void eval(Row[] value) {

        if(value == null || value.length < 1) return;

        try {

            Iterator<Row> iterator = Arrays.stream(value).iterator();

//            Iterator<Row> iterator = value.iterator();

            while (iterator.hasNext()){

                Row row = iterator.next();

                String content_type = String.valueOf(row.getField("content_type"));
                String url  = String.valueOf(row.getField("url"));

                collect(Row.of(content_type,url));

            }


        }catch (Exception e){

        }


    }

//    private void collect(Row of) {
//        collect(of);
//    }


//    @Override
//    public TypeInformation<Row> getResultType() {
//        return super.getResultType();
//    }

    // the automatic, reflection-based type inference is disabled and
    // replaced by the following logic
//    @Override
//    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
//
//        return TypeInference
//                .newBuilder()
//                // specify typed arguments
//                // parameters will be casted implicitly to those types if necessary
//                .typedArguments(DataTypes.ROW(DataTypes.STRING(),DataTypes.STRING()))
//                // specify a strategy for the result data type of the function
//                .outputTypeStrategy(callContext -> {
//
//                    if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
//                        throw callContext.newValidationError("Literal expected for second argument.");
//                    }
//
//                    // return a data type based on a literal
//                    final String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
//                    switch (literal) {
//                        case "INT":
//                            return Optional.of(DataTypes.INT().notNull());
//                        case "DOUBLE":
//                            return Optional.of(DataTypes.DOUBLE().notNull());
//                        case "STRING":
//                        default:
//                            return Optional.of(DataTypes.STRING());
//                    }
//
//                })
//                .build();
//
//    }
}