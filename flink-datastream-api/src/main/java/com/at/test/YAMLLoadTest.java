package com.at.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @create 2022-09-03
 */
public class YAMLLoadTest {

    private static final Logger LOG = LoggerFactory.getLogger(YAMLLoadTest.class);

    // the keys whose values should be hidden
    private static final String[] SENSITIVE_KEYS =
            new String[]{"password", "secret", "fs.azure.account.key", "apikey"};

    // the hidden content to be displayed
    public static final String HIDDEN_CONTENT = "******";

    public static void main(String[] args) {

        Configuration configuration = loadYAMLResource(new File("D:\\workspace\\flink_\\flink-datastream-api\\src\\main\\resources\\application-product.yaml"));

        System.out.println(configuration);


    }


    static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    LOG.info(
                            "Loading configuration property: {}, {}",
                            key,
                            isSensitive(key) ? HIDDEN_CONTENT : value);
                    config.setString(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }

    static boolean isSensitive(String key) {
        Preconditions.checkNotNull(key, "key is null");
        final String keyInLower = key.toLowerCase();
        for (String hideKey : SENSITIVE_KEYS) {
            if (keyInLower.length() >= hideKey.length() && keyInLower.contains(hideKey)) {
                return true;
            }
        }
        return false;
    }

}
