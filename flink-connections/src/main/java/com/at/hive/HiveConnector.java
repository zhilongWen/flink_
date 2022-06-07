package com.at.hive;

import com.at.util.EnvironmentUtil;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.sql.*;

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

        // --execute.mode stream --enable.checkpoint false --enable.table.env true --enable.hive.env true

        EnvironmentUtil.Environment environment = EnvironmentUtil.getExecutionEnvironment(args);
        StreamExecutionEnvironment env = environment.getEnv();

        StreamTableEnvironment tableEnv = environment.getTableEnv();
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);


        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");


        DataStreamSource<Tuple5<Integer, String, Integer, Integer, String>> streamSource = env
                .addSource(
                        new RichSourceFunction<Tuple5<Integer, String, Integer, Integer, String>>() {

                            private Connection connection;
                            private String countSQL;
                            private String querySQL;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                if (connection == null) {
                                    try {
                                        Class.forName("com.mysql.cj.jdbc.Driver");
                                        connection = DriverManager.getConnection(
                                                "jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false",
                                                "root",
                                                "root"
                                        );
                                    } catch (ClassNotFoundException | SQLException e) {
                                        e.printStackTrace();
                                    }
                                }

                                countSQL = "select count(1) from rule_table";
                                querySQL = "select user_id,name,age,sex,address from rule_table";

                            }

                            @Override
                            public void run(SourceContext<Tuple5<Integer, String, Integer, Integer, String>> ctx) throws Exception {

                                PreparedStatement preparedStatement = null;
                                ResultSet resultSet = null;


                                try {

                                    preparedStatement = connection.prepareStatement(querySQL);

                                    resultSet = preparedStatement.executeQuery();

                                    while (resultSet.next()) {

                                        int user_id = resultSet.getInt("user_id");
                                        String name = resultSet.getString("name");
                                        int age = resultSet.getInt("age");
                                        int sex = resultSet.getInt("sex");
                                        String address = resultSet.getString("address");

                                        ctx.collect(Tuple5.of(user_id, name, age, sex, address));
                                    }

                                } catch (SQLException t1) {
                                    t1.printStackTrace();
                                } finally {
                                    try {
                                        preparedStatement.close();
                                        resultSet.close();
                                    } catch (SQLException t2) {
                                        t2.printStackTrace();
                                    }

                                }


                            }

                            @Override
                            public void cancel() {
                                try {
                                    connection.close();
                                } catch (SQLException throwables) {
                                    throwables.printStackTrace();
                                }
                            }
                        });

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);


        Schema schema = Schema
                .newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.INT())
                .column("f3", DataTypes.INT())
                .column("f4", DataTypes.STRING())
                .build();

        Table table = tableEnv
                .fromDataStream(streamSource, schema)
                .as("user_id", "name", "age", "sex", "address");

        /*
21:17:32,033 ERROR org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer [] - org.apache.hadoop.hive.ql.parse.SemanticException: Line 9:6 Table not found 'source_tbl'
	at org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer.getMetaData(HiveParserSemanticAnalyzer.java:1547)
	at org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer.getMetaData(HiveParserSemanticAnalyzer.java:1487)
	at org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer.genResolvedParseTree(HiveParserSemanticAnalyzer.java:2283)
	at org.apache.flink.table.planner.delegation.hive.HiveParserCalcitePlanner.genLogicalPlan(HiveParserCalcitePlanner.java:255)
	at org.apache.flink.table.planner.delegation.hive.HiveParser.analyzeSql(HiveParser.java:290)
	at org.apache.flink.table.planner.delegation.hive.HiveParser.processCmd(HiveParser.java:238)
	at org.apache.flink.table.planner.delegation.hive.HiveParser.parse(HiveParser.java:208)
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:695)
	at com.at.hive.HiveConnector.main(HiveConnector.java:288)

Exception in thread "main" org.apache.flink.table.api.ValidationException: HiveParser failed to parse insert into rule_tbl
select user_id,
       name,
       age,
       sex,
       address,
       from_unixtime(unix_timestamp(), 'yyyyMMdd') as dt,
       '2100'                                      as hm
 from source_tbl
	at org.apache.flink.table.planner.delegation.hive.HiveParser.processCmd(HiveParser.java:253)
	at org.apache.flink.table.planner.delegation.hive.HiveParser.parse(HiveParser.java:208)
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:695)
	at com.at.hive.HiveConnector.main(HiveConnector.java:288)
Caused by: org.apache.hadoop.hive.ql.parse.SemanticException: Line 9:6 Table not found 'source_tbl'
	at org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer.getMetaData(HiveParserSemanticAnalyzer.java:1547)
	at org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer.getMetaData(HiveParserSemanticAnalyzer.java:1487)
	at org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer.genResolvedParseTree(HiveParserSemanticAnalyzer.java:2283)
	at org.apache.flink.table.planner.delegation.hive.HiveParserCalcitePlanner.genLogicalPlan(HiveParserCalcitePlanner.java:255)
	at org.apache.flink.table.planner.delegation.hive.HiveParser.analyzeSql(HiveParser.java:290)
	at org.apache.flink.table.planner.delegation.hive.HiveParser.processCmd(HiveParser.java:238)
	... 3 more

         */


        tableEnv.createTemporaryView("source_tbl", table);
//        tableEnv.executeSql("select user_id,\n"
//                + "       name,\n"
//                + "       age,\n"
//                + "       sex,\n"
//                + "       address,\n"
//                + "       from_unixtime(unix_timestamp(), 'yyyyMMdd') as dt,\n"
//                + "       '2100'                                      as hm\n"
//                + "from source_tbl").print();

        tableEnv
                .executeSql("insert into rule_tbl\n"
                        + "select user_id,\n"
                        + "       name,\n"
                        + "       age,\n"
                        + "       sex,\n"
                        + "       address,\n"
                        + "       from_unixtime(unix_timestamp(), 'yyyyMMdd') as dt,\n"
                        + "       '2100'                                      as hm\n"
                        + " from source_tbl");

        tableEnv.executeSql("select * from rule_tbl").print();


        env.execute();

    }

}
