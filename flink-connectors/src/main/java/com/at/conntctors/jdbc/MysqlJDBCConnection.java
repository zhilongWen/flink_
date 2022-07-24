package com.at.conntctors.jdbc;

import com.at.pojo.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.shaded.guava30.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava30.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @create 2022-06-03
 */
public class MysqlJDBCConnection {

    /*


		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-jdbc</artifactId>
			<version>${flink.version}</version>
			<scope>provided</scope>
		</dependency>

		<!-- https://mvnrepository.com/artifact/mysql/mysql-connector-java -->
		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>8.0.28</version>
		</dependency>


CREATE TABLE userbehavior_tbl(
    user_id int not null,
    item_id bigint,
    category_id int,
    behavior varchar(64),
    ts bigint,
    PRIMARY KEY (user_id)
)

     */


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> buyStream = env
                .readTextFile("D:\\workspace\\flink_\\files\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] elems = value.split(",");
                        return UserBehavior.builder()
                                .userId(Integer.parseInt(elems[0]))
                                .itemId(Long.parseLong(elems[1]))
                                .categoryId(Integer.parseInt(elems[2]))
                                .behavior(elems[3])
                                .ts(Long.parseLong(elems[4]))
                                .build();
                    }
                })
                .filter(r -> "buy".equals(r.behavior))
                .flatMap(new RichFlatMapFunction<UserBehavior, UserBehavior>() {

                    private transient Set<Integer> set;
                    private transient BloomFilter<Integer> bloomFilter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        set = new HashSet<>();
                        bloomFilter = BloomFilter.create(Funnels.integerFunnel(), 1000000, 0.01);
                    }

                    @Override
                    public void flatMap(UserBehavior value, Collector<UserBehavior> out) throws Exception {

                        if (!set.contains(value.userId)) {
                            set.add(value.userId);
                            out.collect(value);
                        }

//                        if (!bloomFilter.mightContain(value.userId)) {
//                            bloomFilter.put(value.userId);
//                            out.collect(value);
//                        }

                    }
                });

        buyStream
                .addSink(
                        JdbcSink.sink(
                                "insert into userbehavior_tbl (user_id,item_id,category_id,behavior,ts) values(?,?,?,?,?)",
                                new JdbcStatementBuilder<UserBehavior>() {
                                    @Override
                                    public void accept(PreparedStatement preparedStatement, UserBehavior userBehavior) throws SQLException {
                                        preparedStatement.setInt(1, userBehavior.getUserId());
                                        preparedStatement.setLong(2, userBehavior.getItemId());
                                        preparedStatement.setInt(3, userBehavior.getCategoryId());
                                        preparedStatement.setString(4, userBehavior.getBehavior());
                                        preparedStatement.setLong(5, userBehavior.getTs());

                                    }
                                },
                                JdbcExecutionOptions
                                        .builder()
                                        .withBatchSize(100)
                                        .withBatchIntervalMs(50)
                                        .withMaxRetries(3)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false")
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername("root")
                                        .withPassword("root")
                                        .build()
                        )
                );

//        buyStream
//                .addSink(
//                        JdbcSink.exactlyOnceSink(
//                                "insert into userbehavior_tbl (user_id,item_id,category_id,behavior,ts) values(?,?,?,?,?)",
//                                new JdbcStatementBuilder<UserBehavior>() {
//                                    @Override
//                                    public void accept(PreparedStatement preparedStatement, UserBehavior userBehavior) throws SQLException {
//                                        preparedStatement.setInt(1, userBehavior.getUserId());
//                                        preparedStatement.setLong(2, userBehavior.getItemId());
//                                        preparedStatement.setInt(3, userBehavior.getCategoryId());
//                                        preparedStatement.setString(4, userBehavior.getBehavior());
//                                        preparedStatement.setLong(5, userBehavior.getTs());
//
//                                    }
//                                },
//                                JdbcExecutionOptions
//                                        .builder()
////                                        .withBatchSize(100)
////                                        .withBatchIntervalMs(50)
//                                        .withMaxRetries(0) //// JDBC XA sink requires maxRetries equal to 0, otherwise it could cause duplicates. See issue FLINK-22311 for details.
//                                        .build(),
//                                JdbcExactlyOnceOptions
//                                        .builder()
////                                        .withMaxCommitAttempts(3)
////                                        .withAllowOutOfOrderCommits(true)
////                                        .withRecoveredAndRollback(true)
//                                        .withTransactionPerConnection(true)
//                                        .build(),
//                                new SerializableSupplier<XADataSource>() {
//                                    @Override
//                                    public XADataSource get() {
//                                        MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
//                                        xaDataSource.setURL("jdbc:mysql://hadoop102:3306/gmall_report"); // gmall_report?characterEncoding=utf-8&useSSL=false
//                                        xaDataSource.setUser("root");
//                                        xaDataSource.setPassword("root");
//
//                                        return xaDataSource;
//                                    }
//                                }
//                        )
//                );


        env.execute();


    }

}
