package com.at.ml.helloword;

import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;


/**
 *
 * https://nightlies.apache.org/flink/flink-ml-docs-master/docs/try-flink-ml/java/build-your-own-project/
 * https://zhuanlan.zhihu.com/p/110059114?utm_campaign=shareopn&utm_medium=social&utm_oi=1107989909943709696&utm_psn=1613340083008028672&utm_source=wechat_session
 *
 * @author zhilong.wen
 */
public class KMeansExample {


    /*

     <dependencies>
        <!-- Apache Flink dependencies -->
        <!-- These dependencies are provided, because they should not be packaged into the JAR file. -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Add connector dependencies here. They must be in the default scope (compile). -->

        <!-- Example:

		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-kafka</artifactId>
			<version>${flink.version}</version>
		</dependency>
		-->

        <!-- Add logging framework, to produce console output when running in the IDE. -->
        <!-- These dependencies are excluded from the application JAR by default. -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j-impl</artifactId>
            <version>${log4j.version}</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-api</artifactId>
            <version>${log4j.version}</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>${log4j.version}</version>
            <scope>runtime</scope>
        </dependency>

        <!-- flink table -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-loader</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- flink connector -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-files</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- flink ml -->

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-ml-uber</artifactId>
            <version>2.1.0</version>
            <scope>provided</scope>
        </dependency>


        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>statefun-flink-core</artifactId>
            <version>3.2.0</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-streaming-java_2.12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.apache.flink</groupId>
                    <artifactId>flink-metrics-dropwizard</artifactId>
                </exclusion>
            </exclusions>
        </dependency>


    </dependencies>

     */

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String featuresCol = "features";
        String predictionCol = "prediction";

        // Generate train data and predict data as DataStream.
        DataStream<DenseVector> inputStream = env.fromElements(
                Vectors.dense(0.0, 0.0),
                Vectors.dense(0.0, 0.3),
                Vectors.dense(0.3, 0.0),
                Vectors.dense(9.0, 0.0),
                Vectors.dense(9.0, 0.6),
                Vectors.dense(9.6, 0.0)
        );

        // Convert data from DataStream to Table, as Flink ML uses Table API.
        Table input = tEnv.fromDataStream(inputStream).as(featuresCol);

        // Creates a K-means object and initialize its parameters.
        KMeans kmeans = new KMeans()
                .setK(2)
                .setSeed(1L)
                .setFeaturesCol(featuresCol)
                .setPredictionCol(predictionCol);

        // Trains the K-means Model.
        KMeansModel model = kmeans.fit(input);

        // Use the K-means Model for predictions.
        Table output = model.transform(input)[0];

        // Extracts and displays prediction result.
        for (CloseableIterator<Row> it = output.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector vector = (DenseVector) row.getField(featuresCol);
            int clusterId = (Integer) row.getField(predictionCol);
            System.out.println("Vector: " + vector + "\tCluster ID: " + clusterId);
        }
    }
}
