package com.at.iceberg.demo;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @create 2022-06-01
 */
public class ProductDataToKafka {

    private static KafkaProducer<String, String> producer;
    private static final String[] BEHAVIORS = {"pv", "buy", "cart", "fav"};

    private static Random random = new Random();

    private static void write() throws Exception {


        UserBehavior userBehavior = new UserBehavior();
        userBehavior.setUserId(random.nextInt(100));
        userBehavior.setItemId(random.nextLong() & Long.MAX_VALUE);
        userBehavior.setCategoryId(random.nextInt(100));
        userBehavior.setBehavior(BEHAVIORS[random.nextInt(4)]);
        userBehavior.setTs(new Date().getTime());

        String jsonString = JSON.toJSONString(userBehavior);

        //包装成kafka发送的记录
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("user_behaviors", null, null, jsonString);
        //发送到缓存
        producer.send(record);
        //立即发送
        producer.flush();


    }

    public static void main(String[] args) {

        Properties props = new Properties(); //user_behaviors
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);

        while (true) {
            try {
                write();
                try {
                    TimeUnit.SECONDS.sleep(random.nextInt(5));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }


}
