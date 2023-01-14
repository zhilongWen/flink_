package com.at.strategy.v3;

import com.at.t1.p1.FoodRequest;
import com.at.t1.p1.FoodService;

/**
 * @create 2023-01-14
 */
public class Food extends AbstractStrategy implements Strategy {
    private static final Food instance = new Food();
    private FoodService foodService;
    private Food() {
        register();
    }
    public static Food getInstance() {
        return instance;
    }
    @Override
    public void issue(Object... params) {
        FoodRequest request = new FoodRequest(params);
        foodService.getCoupon(request);
    }
}