package com.at.strategy.v3;

/**
 * @create 2023-01-14
 */
public abstract class AbstractStrategy implements Strategy {
    // 类注册方法
    public void register() {
        StrategyContext.registerStrategy(getClass().getSimpleName(), this);
    }
}