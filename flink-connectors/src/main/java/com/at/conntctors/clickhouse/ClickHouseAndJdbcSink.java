package com.at.conntctors.clickhouse;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.w3c.dom.ranges.Range;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zero
 * @create 2022-10-23
 */
public class ClickHouseAndJdbcSink {

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


        SinkFunction<Tuple4<Integer, String, Double, String>> jdbcSink = JdbcSink.<Tuple4<Integer, String, Double, String>>sink(
                "insert into st_order_mt_db01 values(?,?,?,?)",
                new JdbcStatementBuilder<Tuple4<Integer, String, Double, String>>() {
                    @Override
                    public void accept(PreparedStatement statement, Tuple4<Integer, String, Double, String> t) throws SQLException {

                        Field[] fields = t.getClass().getDeclaredFields();

//                        for (int i = 0; i < fields.length; i++) {
//                            try {
//                                statement.setObject((i+1),fields[i].get(t));
//                            } catch (IllegalAccessException e) {
//                                System.out.println(String.format("第 %d 个字段赋值异常，cause by %s",(i+1),e.getMessage()));
//                                e.printStackTrace();
//                            }
//                        }

                        statement.setInt(1, t.f0);
                        statement.setString(2, t.f1);
                        statement.setDouble(3, t.f2);
                        statement.setString(4, t.f3);

                    }
                },
                //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性赋值，执行执行批次大小
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                //构建者设计模式，JdbcConnectionOptions，给连接相关的属性进行赋值
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://hadoop102:8123/testdb01")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()

        );

        source.addSink(jdbcSink);


        env.execute();


    }


}
