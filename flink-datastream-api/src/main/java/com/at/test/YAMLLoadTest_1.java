package com.at.test;


import org.apache.flink.configuration.Configuration;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

/**
 * @create 2022-09-03
 */
public class YAMLLoadTest_1 {

    public static void main(String[] args) throws Exception {

        Yaml yaml = new Yaml();

        String filePath = "D:\\workspace\\flink_\\flink-datastream-api\\src\\main\\resources\\application-product.yaml";

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

//        Configuration configuration = yaml.loadAs(reader, Configuration.class);
//        Properties configuration = yaml.loadAs(reader, Properties.class);
        Map configuration = yaml.loadAs(reader, Map.class);

        System.out.println(configuration);



    }

}
