package com.at.strategy.v2;


import com.at.strategy.v1.FoodRequest;
import com.at.strategy.v1.FoodService;

/**
 * @create 2023-01-14
 */
public class Food implements Strategy {
    private FoodService foodService;
    @Override
    public void issue(Object... params) {
        FoodRequest request = new FoodRequest(params);
        foodService.getCoupon(request);
    }
}
