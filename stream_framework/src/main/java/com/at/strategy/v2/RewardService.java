package com.at.strategy.v2;

/**
 * @create 2023-01-14
 */
public class RewardService {
    public void issueReward(String rewardType, Object ... params) {
        Strategy strategy = StrategyContext.getStrategy(rewardType);
        strategy.issue(params);
    }
}
