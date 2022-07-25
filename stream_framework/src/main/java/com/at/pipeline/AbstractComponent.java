package com.at.pipeline;

import java.util.Collection;

/**
 * @create 2022-07-25
 */
public abstract class AbstractComponent<T,R> implements Component<T> {


    @Override
    public void execute(T t) {

        // 执行当前组件
        R r = doExecute(t);

        System.out.println("--------------- " + getName() + " receive " + t + " return " + r );

        // 获取下游组件，并执行
        Collection<Component> downStreams = getDownStreams();

//        if (!CollectionUtils.isEmpty(downStreams)) {
        if (downStreams != null && !downStreams.isEmpty()) {
            downStreams.forEach(c -> c.execute(r));
        }


    }

    /**
     * 具体组件执行处理
     * @param t
     * @return
     */
    protected abstract R doExecute(T t);


    @Override
    public void startup() {

        // 下游 -> 上游 依次启动

        Collection<Component> downStreams = getDownStreams();

        if(downStreams != null && !downStreams.isEmpty()){
            downStreams.forEach(Component::startup);
        }

        // do startup
        System.out.println("--------- " + getName() + " is start --------- ");

    }

    @Override
    public void shutdown() {

        // 上游 -> 下游 依次关闭
        // do shutdown
        System.out.println("--------- " + getName() + " is shutdown --------- ");

        Collection<Component> downStreams = getDownStreams();

        if(downStreams != null && !downStreams.isEmpty()){
            downStreams.forEach(Component::shutdown);
        }


    }
}
