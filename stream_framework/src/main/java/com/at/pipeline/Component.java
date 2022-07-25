package com.at.pipeline;

import java.util.Collection;

/**
 * @create 2022-07-25
 *
 * 组件
 *
 */
public interface Component<T> extends LifeCycle{

    /**
     * 组件名字
     * @return
     */
    String getName();


    /**
     * 获取下游组件
     * @return
     */
    Collection<Component> getDownStreams();


    /**
     * 执行
     */
    void execute(T t);

}
