package com.at.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @create 2022-06-07
 */
public class HiveConnector {

    /*
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-api-java-bridge</artifactId>
			<version>1.15.0</version>
			<scope>provided</scope>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-planner_${scala.binary.version}</artifactId>
			<version>${flink.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-api-java</artifactId>
			<version>${flink.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-hive_2.12</artifactId>
			<version>1.15.0</version>
			<scope>provided</scope>
		</dependency>

		<!-- Exception in thread "main" java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)V -->
		<dependency>
			<groupId>com.google.guava</groupId>
			<artifactId>guava</artifactId>
			<version>30.0-jre</version>
		</dependency>

		<!-- Hive Dependency -->
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-exec</artifactId>
			<version>3.1.2</version>
			<scope>provided</scope>
		</dependency>


		<!-- hadoop -->
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-hdfs</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-yarn-client</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-core</artifactId>
			<version>${hadoop.version}</version>
		</dependency>
     */

    /*

		<!-- hive -->
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-hive_2.12</artifactId>
			<version>1.15.0</version>
			<scope>provided</scope>
			<exclusions>
				<exclusion>
					<groupId>org.apache.thrift</groupId>
					<artifactId>libfb303</artifactId>
				</exclusion>
			</exclusions>
		</dependency>

		<!-- Exception in thread "main" java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)V -->
		<dependency>
			<groupId>com.google.guava</groupId>
			<artifactId>guava</artifactId>
			<version>30.0-jre</version>
		</dependency>

		<!-- Hive Dependency -->
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-exec</artifactId>
			<version>3.1.2</version>
			<scope>provided</scope>
			<exclusions>
				<exclusion>
					<groupId>org.apache.thrift</groupId>
					<artifactId>libfb303</artifactId>
				</exclusion>
			</exclusions>
		</dependency>

		<dependency>
			<groupId>org.apache.thrift</groupId>
			<artifactId>libfb303</artifactId>
			<version>0.9.3</version>
		</dependency>


		<!-- hadoop -->
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-hdfs</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-yarn-client</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-core</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

     */


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive",hive);


        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");

        tableEnv.executeSql("select * from tbl").print();
    }

}
