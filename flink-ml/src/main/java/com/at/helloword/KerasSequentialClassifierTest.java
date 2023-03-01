package com.at.helloword;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToTensorBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.KerasSequentialClassificationModel;
import com.alibaba.alink.pipeline.classification.KerasSequentialClassifier;

/**
 * @create 2023-03-02
 */
public class KerasSequentialClassifierTest {
    public static void main(String[] args) throws Exception {
        BatchOperator<?> source = new CsvSourceBatchOp()
                .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
                .setSchemaStr("tensor string, label int");
        source = new ToTensorBatchOp()
                .setSelectedCol("tensor")
                .setTensorDataType("DOUBLE")
                .setTensorShape(200, 3)
                .linkFrom(source);
        KerasSequentialClassifier trainer = new KerasSequentialClassifier()
                .setTensorCol("tensor")
                .setLabelCol("label")
                .setLayers(new String[] {
                        "Conv1D(256, 5, padding='same', activation='relu')",
                        "Conv1D(128, 5, padding='same', activation='relu')",
                        "Dropout(0.1)",
                        "MaxPooling1D(pool_size=8)",
                        "Conv1D(128, 5, padding='same', activation='relu')",
                        "Conv1D(128, 5, padding='same', activation='relu')",
                        "Flatten()"
                })
                .setOptimizer("Adam()")
                .setNumEpochs(1)
                .setPredictionCol("pred")
                .setPredictionDetailCol("pred_detail")
                .setReservedCols("label");
        KerasSequentialClassificationModel model = trainer.fit(source);
        BatchOperator <?> prediction = model.transform(source);
        prediction.lazyPrint(10);
        BatchOperator.execute();
    }
}
