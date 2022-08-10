package com.at.factory.abstractfactory;

/**
 * @create 2022-08-10
 */
public class SimpleFactory {

    public static Factory create(Factory factory){
        return factory;
    }

}
