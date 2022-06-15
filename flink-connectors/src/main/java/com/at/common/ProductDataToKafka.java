package com.at.common;

import com.alibaba.fastjson2.JSON;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @create 2022-06-01
 */
public class ProductDataToKafka {

    private static KafkaProducer<String, String> producer;
    private static final String[] BEHAVIORS = {"pv","buy","cart", "fav"};

    private  static Random random = new Random();

    private static void write() throws Exception{


        UserBehavior userBehavior = UserBehavior
                .builder()
                .userId(random.nextInt(100))
                .itemId(random.nextLong() & Long.MAX_VALUE)
                .categoryId(random.nextInt(100))
                .behavior(BEHAVIORS[random.nextInt(4)])
                .ts(new Date().getTime())
                .build();


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

        while (true){
            try {
                write();
                try { TimeUnit.SECONDS.sleep(random.nextInt(5)); } catch (InterruptedException e) { e.printStackTrace(); }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @Data
    public static class UserBehavior {

        //用户ID
        public int userId;
        //商品ID
        public long itemId;
        //商品类目ID
        public  int categoryId;
        //用户行为，包括{"pv","buy","cart", "fav"}
        public String behavior;
        //行为发生的时间戳，单位秒
        public long ts;

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId=" + userId +
                    ", itemId=" + itemId +
                    ", categoryId=" + categoryId +
                    ", behavior='" + behavior + '\'' +
                    ", ts=" + new Timestamp(ts) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserBehavior that = (UserBehavior) o;
            return userId == that.userId && itemId == that.itemId && categoryId == that.categoryId && ts == that.ts && Objects.equals(behavior, that.behavior);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userId, itemId, categoryId, behavior, ts);
        }
    }



}
