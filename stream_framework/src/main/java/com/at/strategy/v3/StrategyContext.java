package com.at.strategy.v3;

import java.util.HashMap;
import java.util.Map;

/**
 * @create 2023-01-14
 */
public class StrategyContext {

    private static final Map<String, Strategy> registerMap = new HashMap<>();

    // 注册策略
    public static void registerStrategy(String rewardType, Strategy strategy) {
        registerMap.putIfAbsent(rewardType, strategy);
    }

    // 获取策略
    public static Strategy getStrategy(String rewardType) {
        return registerMap.get(rewardType);
    }


}
