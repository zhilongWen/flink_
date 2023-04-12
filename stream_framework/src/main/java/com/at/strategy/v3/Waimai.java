package com.at.strategy.v3;

import com.at.strategy.v1.WaimaiRequest;
import com.at.strategy.v1.WaimaiService;

/**
 * @create 2023-01-14
 */
public class Waimai extends AbstractStrategy implements Strategy {

    private static final Waimai instance = new Waimai();

    private WaimaiService waimaiService;

    private Waimai() {
        register();
    }

    public static Waimai getInstance() {
        return instance;
    }

    @Override
    public void issue(Object... params) {
        WaimaiRequest request = new WaimaiRequest();
        // 构建入参
        request.setWaimaiReq(params);
        waimaiService.issueWaimai(request);
    }
}
