package com.at.strategy;

import java.lang.reflect.InvocationTargetException;

/**
 * @create 2022-07-31
 */
public class Main {

    public static void main(String[] args) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        String strategy = "C";

        if ("Strategy".equals(strategy)) {
            System.out.println("买一送一");
        } else if ("B".equals(strategy)) {
            System.out.println("满100减50");
        } else if ("C".equals(strategy)) {
            System.out.println("满1000减150");
        } else if ("D".equals(strategy)) {
            System.out.println("免费");
        }


        System.out.println("=========================");


        com.at.strategy.Strategy.instance("C",(t -> t.strategy()));


    }

}
