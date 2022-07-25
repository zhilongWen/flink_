package com.at.pipeline.test;

import com.at.pipeline.LifeCycle;
import com.at.pipeline.Source;

/**
 * @create 2022-07-25
 */
public class Pipeline implements LifeCycle {


    private Source source;

    public Pipeline(Source source) {
        this.source = source;
    }


    @Override
    public void init(String config) {
        // 初始化
        System.out.println("--------- Pipeline init --------- ");
        source.init(null);
    }


    @Override
    public void startup() {
        // 启动
        System.out.println("--------- Pipeline startup --------- ");
        source.startup();
    }

    @Override
    public void shutdown() {
        // 结束
        source.shutdown();
        System.out.println("--------- Pipeline shutdown --------- ");
    }
}
