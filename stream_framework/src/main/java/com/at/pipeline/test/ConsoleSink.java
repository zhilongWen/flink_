package com.at.pipeline.test;

import com.at.pipeline.Component;
import com.at.pipeline.Sink;

import java.util.Collection;

/**
 * @create 2022-07-25
 */
public class ConsoleSink extends Sink<String, Void> {

    @Override
    protected Void doExecute(String s) {
        return null;
    }

    @Override
    public String getName() {
        return "Console-Sink";
    }

    @Override
    public Collection<Component> getDownStreams() {
        return null;
    }

    @Override
    public void init(String config) {

    }
}
