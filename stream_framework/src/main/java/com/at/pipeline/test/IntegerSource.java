package com.at.pipeline.test;

import com.at.pipeline.Component;
import com.at.pipeline.Source;

import java.util.Collection;
import java.util.Collections;

/**
 * @create 2022-07-25
 */
public class IntegerSource extends Source<Integer,Integer> {


    private int val = 0;

    @Override
    protected Integer doExecute(Integer o) {
        return o;
    }

    @Override
    public void init(String config) {
        System.out.println("--------- " + getName() + " init --------- ");
        val = 1;
    }

    @Override
    public void startup() {
        super.startup();
        execute(val);
    }

    @Override
    public String getName() {
        return "Integer-Source";
    }

    @Override
    public Collection<Component> getDownStreams() {
        return Collections.singletonList(new IncrChannel());
    }
}
