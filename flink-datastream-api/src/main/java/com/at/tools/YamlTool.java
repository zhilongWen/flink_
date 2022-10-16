package com.at.tools;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author zero
 * @create 2022-10-16
 */
public class YamlTool {

    // https://cloud.tencent.com/developer/article/1591043

    /**
     * 声明一个Map存解析之后的内容
     */
    private Map<String, Object> properties;

    /**
     * 空的构造函数
     */
    public YamlTool() {
    }

    /**
     * 以文件路径为条件的构造函数
     *
     * @param filePath yaml 文件路径
     */
    public YamlTool(String filePath) {

        try (FileInputStream inputStream = new FileInputStream(filePath)) {
            // 调基础工具类的方法
            Yaml yaml = new Yaml();
            properties = yaml.loadAs(inputStream, Map.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 从String 中获取配置的方法
     *
     * @param content
     */
    public void initWithString(String content) {
        // 这里的yaml和上面那个应该可以抽取成一个全局变量,不过平常用的时候
        // 也不会两个同时用
        Yaml yaml = new Yaml();
        properties = yaml.loadAs(content, Map.class);
    }


    /**
     * 从Map中获取配置的值
     * 传的key支持两种形式, 一种是单独的,如user.path.key
     * 一种是获取数组中的某一个,如 user.path.key[0]
     *
     * @param key
     * @return
     */
    public <T> T getValueByKey(String key, T defaultValue) {
        String separator = ".";
        String[] separatorKeys = null;
        if (key.contains(separator)) {
            // 取下面配置项的情况, user.path.keys 这种
            separatorKeys = key.split("\\.");
        } else {
            // 直接取一个配置项的情况, user
            Object res = properties.get(key);
            return res == null ? defaultValue : (T) res;
        }
        // 下面肯定是取多个的情况
        String finalValue = null;
        Object tempObject = properties;
        for (int i = 0; i < separatorKeys.length; i++) {
            //如果是user[0].path这种情况,则按list处理
            String innerKey = separatorKeys[i];
            String index = null;
            if (innerKey.contains("[")) {
                // 如果是user[0]的形式,则index = 0 , innerKey=user
                // 如果是user[name]的形式,则index = name , innerKey=user
                index = StringUtils.substringsBetween(innerKey, "[", "]")[0];

                innerKey = innerKey.substring(0, innerKey.indexOf("["));
            }
            Map<String, Object> mapTempObj = (Map) tempObject;
            Object object = mapTempObj.get(innerKey);
            // 如果没有对应的配置项,则返回设置的默认值
            if (object == null) {
                return defaultValue;
            }
            Object targetObj = object;
            if (index != null) {
                // 如果是取的数组中的值,在这里取值
                if (StringUtils.isNumeric(index)) {
                    targetObj = ((ArrayList) object).get(Integer.valueOf(index));
                } else {

                    final ArrayList<Map<String, Object>> mapArrayList = (ArrayList<Map<String, Object>>) object;

                    for (Map<String, Object> objectMap : mapArrayList) {
                        if (objectMap.keySet().contains(index)) {
                            targetObj = objectMap.get(index);
                            break;
                        }
                    }

                }
            }
            // 一次获取结束,继续获取后面的
            tempObject = targetObj;
            if (i == separatorKeys.length - 1) {
                //循环结束
                return (T) targetObj;
            }

        }
        return null;
    }


}
