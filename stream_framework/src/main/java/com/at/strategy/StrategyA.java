package com.at.strategy;

/**
 * @create 2022-07-31
 */
public class StrategyA implements Strategy{
    @Override
    public void strategy() {
        System.out.println("买一送一");
    }

    @Override
    public String getName() {
        Strategy.super.getName();
        return "nameA";
    }
}
