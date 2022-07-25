package com.at.pipeline.test;

import com.at.pipeline.Channel;
import com.at.pipeline.Component;

import java.util.Collection;
import java.util.Collections;

/**
 * @create 2022-07-25
 */
public class IncrChannel extends Channel<Integer, Integer> {

    @Override
    protected Integer doExecute(Integer integer) {
        return integer + 1;
    }

    @Override
    public String getName() {
        return "Incr-Channel";
    }

    @Override
    public Collection<Component> getDownStreams() {
        return Collections.singletonList(new StringChannel());
    }

    @Override
    public void init(String config) {

    }
}
