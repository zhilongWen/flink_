package com.at.conntctors.clickhouse.ckjdbc;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @create 2023-12-10
 */
public class ClickhouseSinktest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        DataStreamSource<Tuple4<Integer, String, Double, String>> source = env
                .addSource(new SourceFunction<Tuple4<Integer, String, Double, String>>() {

                    private AtomicBoolean isRunning = new AtomicBoolean(true);
                    Random random = new Random();
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
                                TimeUnit.SECONDS.sleep(2);
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


                        statement.setInt(1, t.f0);
                        statement.setString(2, t.f1);
                        statement.setDouble(3, t.f2);
                        statement.setString(4, t.f3);

                    }
                },
                //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性赋值，执行执行批次大小
                new JdbcExecutionOptions.Builder().withBatch(1).build(),
                //构建者设计模式，JdbcConnectionOptions，给连接相关的属性进行赋值
                new JdbcConnectionOptions.Builder()
                        .withUrl("jdbc:clickhouse://hadoop102:8123/testdb01")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()

        );

        source.addSink(jdbcSink);


        env.execute();


    }


}
