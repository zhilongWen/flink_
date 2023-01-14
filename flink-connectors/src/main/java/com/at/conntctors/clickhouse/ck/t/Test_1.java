package com.at.conntctors.clickhouse.ck.t;


import com.at.conntctors.clickhouse.ck.ClickHouseOutputFormat;
import com.at.conntctors.clickhouse.ck.ClickHouseStatementBuilder;
import com.at.conntctors.clickhouse.ck.GenericClickHouseSinkFunction;
import com.at.conntctors.clickhouse.ck.executor.ClickHouseBatchStatementExecutor;
import com.at.conntctors.clickhouse.ck.options.ClickHouseConnectorOptions;
import com.at.conntctors.clickhouse.ck.options.ClickHouseExecutionOptions;
import com.at.conntctors.clickhouse.ck.provider.ClickHouseConnectionProvider;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author zero
 * @create 2022-10-23
 */
public class Test_1 {

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

        GenericClickHouseSinkFunction<Tuple4<Integer, String, Double, String>> sinkFunction = new GenericClickHouseSinkFunction<Tuple4<Integer, String, Double, String>>(
                new ClickHouseOutputFormat<>(
                        new ClickHouseConnectionProvider(
                                new ClickHouseConnectorOptions.ClickHouseConnectorOptionsBuilder()
                                        .withConnectionCheckTimeoutSeconds(60)
                                        .withUrl("jdbc:clickhouse://hadoop102:8123/testdb01")
                                        .build()
                        ),
                        ClickHouseExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(10L)
                                .withMaxRetries(3)
                                .build(),
                        new ClickHouseOutputFormat.StatementExecutorFactory<ClickHouseBatchStatementExecutor<Tuple4<Integer, String, Double, String>>>() {
                            @Override
                            public ClickHouseBatchStatementExecutor<Tuple4<Integer, String, Double, String>> apply(RuntimeContext runtimeContext) {
                                return ClickHouseBatchStatementExecutor.simple(
                                        "insert into st_order_mt_db01 values(?,?,?,?)",
                                        new ClickHouseStatementBuilder<Tuple4<Integer, String, Double, String>>() {
                                            @Override
                                            public void accept(PreparedStatement statement, Tuple4<Integer, String, Double, String> t) throws SQLException {

                                                Field[] fields = t.getClass().getDeclaredFields();

                                                statement.setInt(1, t.f0);
                                                statement.setString(2, t.f1);
                                                statement.setDouble(3, t.f2);
                                                statement.setString(4, t.f3);

                                            }
                                        },
                                        Function.identity());
                            }
                        },
                        ClickHouseOutputFormat.RecordExtractor.identity()
                )
        );


//        source.print();
        source.addSink(sinkFunction);


        env.execute();


    }


}
