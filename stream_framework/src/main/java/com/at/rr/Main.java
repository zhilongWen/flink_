package com.at.rr;

import org.reflections.Reflections;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws Exception {

        String type = "A";
        //
        //A instance = A.instance;
        //B instance1 = B.getInstance();

        Strategy strategy = StrategyContext.getStrategy(type);

        strategy.etl();

        //Reflections reflections = new Reflections(Strategy.class.getPackage().getName());
        //Set<Class<? extends Strategy>> subTypesOf = reflections.getSubTypesOf(Strategy.class);
        //
        //for (Class<? extends Strategy> aClass : subTypesOf) {
        //    if (!aClass.isInterface() && !Modifier.isAbstract(aClass.getModifiers())){
        //        Strategy strategy = aClass.newInstance();
        //        System.out.println(strategy);
        //        System.out.println(strategy.getType());
        //    }
        //}

    }

}
