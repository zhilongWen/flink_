package com.at.tools;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @create 2022-10-16
 */
public class YamlToolTest {

    YamlTool yamlTools;

    @Before
    public void setUp() {
        yamlTools = new YamlTool("D:\\workspace\\flink_\\flink-datastream-api\\src\\main\\resources\\dev.yaml");
    }

    @After
    public void tearDown() throws Exception {
        yamlTools = null;
    }

    @Test
    public void test4(){

        System.out.println(yamlTools.getValueByKey("config[0].dev.name","dev-name-1"));
        System.out.println(yamlTools.getValueByKey("config[dev].dev.name","dev-name-2"));

        System.out.println(yamlTools.getValueByKey("config[dev].dev.type[1]", "dev-p1"));

        System.out.println(yamlTools.getValueByKey("config[dev].dev.port", 0000));

        System.out.println(yamlTools.getValueByKey("config[production].production.port[0]", 9));

        System.out.println("==================================");
        List<Integer> defaultValue = Arrays.asList(1, 2, 3, 4);
        System.out.println(yamlTools.getValueByKey("config[production].production.port", defaultValue));


    }

    @Test
    public void test3(){

        System.out.println(yamlTools.getValueByKey("config[0].dev.name", "dev-name"));

        System.out.println(yamlTools.getValueByKey("config[0].dev.type[1]", "dev-p1"));

        System.out.println(yamlTools.getValueByKey("config[0].dev.port", 0000));

        System.out.println(yamlTools.getValueByKey("config[1].production.port[0]", 9));

        System.out.println("==================================");
        List<Integer> defaultValue = Arrays.asList(1, 2, 3, 4);
        System.out.println(yamlTools.getValueByKey("config[1].production.port", defaultValue));


    }

    @Test
    public void test2(){

        System.out.println(yamlTools.getValueByKey("server.datasource.mysql[1]", "asd"));

        System.out.println(yamlTools.getValueByKey("server.datasource.redis-cluster[0]", "redis"));

        System.out.println(yamlTools.getValueByKey("server.datasource.kafka-servers[2]", "kafka"));

        System.out.println(yamlTools.getValueByKey("server.datasource.clients[0]", 0));
        System.out.println(yamlTools.getValueByKey("server.datasource.clients[2]", 2));
        System.out.println(yamlTools.getValueByKey("server.datasource.clients[4]", 4));


    }

    @Test
    public void test1(){

        final Integer date = yamlTools.getValueByKey("date", 20221010);
        System.out.println(date);

        System.out.println(yamlTools.getValueByKey("app.name", "oop"));

        System.out.println(yamlTools.getValueByKey("redis.cluster.node", "SD001"));

        System.out.println(yamlTools.getValueByKey("redis.password", ""));

        System.out.println(yamlTools.getValueByKey("redis.password-xc","无法获取redis密码"));

        System.out.println(yamlTools.getValueByKey("redis.timeout",100));

//        System.out.println(yamlTools.getValueByKey("redis.jedis.pool", "sdasd"));
        System.out.println(yamlTools.getValueByKey("redis.jedis.pool.max-active", 0));

    }

}
