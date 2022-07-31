package com.at.strategy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Consumer;

/**
 * @create 2022-07-31
 */
public interface Strategy {

    static final String CLASS_NAME_PREFIX = "com.at.strategy.Strategy";

    void strategy();

    default String getName() {
        return null;
    }


    static void instance(String strategyClass, Consumer<Strategy> strategyConsumer) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Class<?> aClass = Class.forName(CLASS_NAME_PREFIX+strategyClass);

        Constructor<?> constructor = aClass.getConstructor();

        Strategy strategyInstance = (Strategy) constructor.newInstance(null);

        strategyConsumer.accept(strategyInstance);

    }


}
