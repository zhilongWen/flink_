package com.at.util;

import com.at.constant.PropertiesConstants;
import javolution.io.Struct;
import org.apache.flink.api.java.utils.ParameterTool;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @create 2022-06-07
 */
public class CheckNullUtil {

    public static <T> T checkArgument(String condition,T reference) {

        if(condition == null) return reference;
        else {

            return reference;
        }
    }

    public static boolean checkArgument(String condition){

        if(condition == null){
            return false;
        }else {
            return Boolean.parseBoolean(condition);
        }

    }


    public static void main(String[] args) {

        ParameterTool parameterTool = buildParameterTool(args);

//        String enableTableEnv = parameterTool.get(PropertiesConstants.ENABLE_TABLE_ENV);
//
//        System.out.println(enableTableEnv);
//
//        boolean aBoolean = parameterTool.getBoolean(PropertiesConstants.ENABLE_TABLE_ENV);

//        Class<Boolean> booleanClass = checkArgument(parameterTool.get(PropertiesConstants.ENABLE_TABLE_ENV), Boolean.class);
//
//        System.out.println(booleanClass);
//
//        String t = "true";
//
//        System.out.println(Boolean.parseBoolean(t));

        boolean argument = checkArgument(parameterTool.get(PropertiesConstants.ENABLE_TABLE_ENV));

        System.out.println(argument);


    }

    private static ParameterTool buildParameterTool(final String[] args) {
        try {
            return ParameterTool
                    .fromPropertiesFile(EnvironmentUtil.class.getResourceAsStream(PropertiesConstants.DEFAULT_PROPERTIES))
                    .mergeWith(ParameterTool.fromArgs(args))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromSystemProperties());
    }

}
