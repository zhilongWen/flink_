package com.at.strategy;

/**
 * @create 2022-07-31
 */
public class StrategyB implements Strategy {
    @Override
    public void strategy() {
        System.out.println("满100减50");
    }

    @Override
    public String getName() {
        Strategy.super.getName();
        return "nameB";
    }
}
