package com.at.helloword;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.classification.XGBoostClassifier;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @create 2023-03-01
 */

public class _01_XGBoost二分类训练 {

    public static void main(String[] args) throws Exception {

        List<Row> rowsData = Arrays
                .asList(
                        Row.of(0, 1, 1.1, 1.0),
                        Row.of(1, -2, 0.9, 2.0),
                        Row.of(0, 100, -0.01, 3.0),
                        Row.of(1, -99, 0.1, 4.0),
                        Row.of(0, 1, 1.1, 5.0),
                        Row.of(1, -2, 0.9, 6.0)
                );

        MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(rowsData, "y int,x1 int,x2 double,x3 double");
        MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(rowsData, "y int,x1 int,x2 double,x3 double");

        XGBoostClassifier xgBoostClassifier = new XGBoostClassifier()
                .setNumRound(1)
                .setPluginVersion("1.5.1")
                .setLabelCol("y")
                .setPredictionDetailCol("pred_detail")
                .setPredictionCol("pred");

        xgBoostClassifier
                .fit(memSourceBatchOp)
                .transform(memSourceStreamOp)
                .print();
        StreamOperator.execute();

    }

}
