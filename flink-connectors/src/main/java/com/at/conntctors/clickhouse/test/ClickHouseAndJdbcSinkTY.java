package com.at.conntctors.clickhouse.test;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zero
 * @create 2022-10-23
 */
public class ClickHouseAndJdbcSinkTY {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Row> source = env
                .addSource(new SourceFunction<Row>() {

                    private AtomicBoolean isRunning = new AtomicBoolean(true);
                    //                    private SplittableRandom random = new SplittableRandom();
                    Random random = new Random();
                    //                    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


                    @Override
                    public void run(SourceContext<Row> ctx) throws Exception {

                        while (isRunning.get()) {
                            int id = random.nextInt() & Integer.MAX_VALUE;
                            String skuId = UUID.randomUUID().toString().substring(0, 3);
                            double totalAmount = 123.00D; //random.nextDouble();
                            String createTime = formatter.format(new Date());

                            Tuple4<Integer, String, Double, String> tuple4 = Tuple4.of(id, skuId, totalAmount, createTime);

                            System.out.println(tuple4);

                            Row of = Row.of(id, skuId, totalAmount, createTime);

                            ctx.collect(of);

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

        source.addSink(
                new ClickHouseJDBCSinkFunction(
                        new ClickHouseJDBCOutputFormat(
                                null,
                                null,
                                new String[]{"hadoop102:8123"},
                                10L,
                                1,
                                "testdb01",
                                "st_order_mt_db01_all",
                                new String[]{"id","sku_id","total_amount","create_time"})
                )
        );

        env.execute();


    }


}
