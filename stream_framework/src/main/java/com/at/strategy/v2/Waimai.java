package com.at.strategy.v2;


import com.at.strategy.v1.WaimaiRequest;
import com.at.strategy.v1.WaimaiService;

/**
 * @create 2023-01-14
 */
public class Waimai implements Strategy {

    private WaimaiService waimaiService;

    @Override
    public void issue(Object... params) {
        WaimaiRequest request = new WaimaiRequest();
        // 构建入参
        request.setWaimaiReq(params);
        waimaiService.issueWaimai(request);
    }
}
