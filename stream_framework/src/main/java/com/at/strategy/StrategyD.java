package com.at.strategy;

/**
 * @create 2022-07-31
 */
public class StrategyD implements Strategy {
    @Override
    public void strategy() {
        System.out.println("免费");
    }

    @Override
    public String getName() {
        Strategy.super.getName();
        return this.getClass().getName();
    }
}
