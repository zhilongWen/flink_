package com.at.rr;

import org.reflections.Reflections;

import java.util.Set;

public interface Strategy {

    void etl();

    String getType();

    static void registerStrategy() throws InstantiationException, IllegalAccessException {


    }

}
