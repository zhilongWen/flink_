package com.at.conntctors.clickhouse;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zero
 * @create 2022-10-23
 */
public class ClickHouseAndJdbcSinkFunction {

    /*

    <!-- clickhouse -->
		<dependency>
			<groupId>ru.yandex.clickhouse</groupId>
			<artifactId>clickhouse-jdbc</artifactId>
			<version>0.2.4</version>
			<exclusions>
				<exclusion>
					<groupId>com.fasterxml.jackson.core</groupId>
					<artifactId>jackson-databind</artifactId>
				</exclusion>
				<exclusion>
					<groupId>com.fasterxml.jackson.core</groupId>
					<artifactId>jackson-core</artifactId>
				</exclusion>
			</exclusions>
		</dependency>


		zk.sh start

		systemctl start clickhouse-server

		clickhouse-client -m

		create database testdb01;

		use testdb01;
		# 创建分布式表
        create table st_order_mt_db01_all on cluster gmall_cluster
        (
            id UInt32,
            sku_id String,
            total_amount Decimal(16,2),
            create_time  Datetime
        )engine = Distributed(gmall_cluster,testdb01, st_order_mt_db01,hiveHash(sku_id))
        ;

        # 创建本地表
        create table st_order_mt_db01 on cluster gmall_cluster
        (
            id UInt32,
            sku_id String,
            total_amount Decimal(16,2),
            create_time  Datetime
        ) engine =ReplicatedMergeTree('/clickhouse/tables/{shard}/st_order_mt_db01','{replica}')
        partition by toYYYYMMDD(create_time)
        primary key (id)
        order by (id,sku_id)
        ;

        insert into st_order_mt_db01 values(?,?,?,?)





     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Tuple4<Integer, String, Double, String>> source = env
                .addSource(new SourceFunction<Tuple4<Integer, String, Double, String>>() {

                    private AtomicBoolean isRunning = new AtomicBoolean(true);
                    //                    private SplittableRandom random = new SplittableRandom();
                    Random random = new Random();
                    //                    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


                    @Override
                    public void run(SourceContext<Tuple4<Integer, String, Double, String>> ctx) throws Exception {

                        while (isRunning.get()) {
                            int id = random.nextInt() & Integer.MAX_VALUE;
                            String skuId = UUID.randomUUID().toString().substring(0, 3);
                            double totalAmount = 123.00D; //random.nextDouble();
                            String createTime = formatter.format(new Date());

                            Tuple4<Integer, String, Double, String> tuple4 = Tuple4.of(id, skuId, totalAmount, createTime);

                            System.out.println(tuple4);

                            ctx.collect(tuple4);

                            try {
                                TimeUnit.SECONDS.sleep(5);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning.set(false);
                    }
                });



        source.addSink(new ClickHouseSinkFunction());


        env.execute();


    }

   static class ClickHouseSinkFunction extends RichSinkFunction<Tuple4<Integer, String, Double, String>> {

        // https://cloud.tencent.com/developer/article/1848930


        Connection connection;
        PreparedStatement pstmt;

        private Connection getConnection() {
            Connection conn = null;
            try {
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                String url = "jdbc:clickhouse://hadoop102:8123/testdb01";
                conn = DriverManager.getConnection(url);

            } catch (Exception e) {
                e.printStackTrace();
            }
            return conn;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "insert into st_order_mt_db01 values(?,?,?,?)";
            pstmt = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(Tuple4<Integer, String, Double, String> value, SinkFunction.Context context) throws Exception {

            pstmt.setInt(1, value.f0);
            pstmt.setString(2, value.f1);
            pstmt.setDouble(3, value.f2);
            pstmt.setString(4, value.f3);

            pstmt.execute();

        }

        @Override
        public void close() throws Exception {
            super.close();
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }



}
