package com.at.pipeline;

/**
 * @create 2022-07-25
 *
 * 生命周期
 *
 * https://zhuanlan.zhihu.com/p/355034910
 * https://developer.aliyun.com/article/972730
 *
 */
public interface LifeCycle {



    /**
     * 初始化
     * @param config
     */
    void init(String config);


    /**
     * 启动
     */
    void startup();


    /**
     * 结束
     */
    void shutdown();


}
