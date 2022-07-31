package com.at.strategy;

/**
 * @create 2022-07-31
 */
public class StrategyC implements Strategy {
    @Override
    public void strategy() {
        System.out.println("满1000减150");
    }

    @Override
    public String getName() {
        Strategy.super.getName();
        return "nameC";
    }
}
