package com.at.pipeline.test;

import com.at.pipeline.Channel;
import com.at.pipeline.Component;

import java.util.Collection;
import java.util.Collections;

/**
 * @create 2022-07-25
 */
public class StringChannel extends Channel<Integer, String> {

    @Override
    protected String doExecute(Integer integer) {
        return "str-" + integer;
    }

    @Override
    public String getName() {
        return "String-Channel";
    }


    @Override
    public Collection<Component> getDownStreams() {
        return Collections.singletonList(new ConsoleSink());
    }

    @Override
    public void init(String config) {

    }
}
