package com.at.pipeline.test;

/**
 * @create 2022-07-25
 */
public class Main {

    public static void main(String[] args) {

        Pipeline pipeline = new Pipeline(new IntegerSource());
        pipeline.init(null);
        pipeline.startup();
        pipeline.shutdown();

    }

}
