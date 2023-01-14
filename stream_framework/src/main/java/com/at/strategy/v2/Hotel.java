package com.at.strategy.v2;

import com.at.t1.p1.HotelRequest;
import com.at.t1.p1.HotelService;

/**
 * @create 2023-01-14
 */
public class Hotel implements Strategy {
    private HotelService hotelService;
    @Override
    public void issue(Object... params) {
        HotelRequest request = new HotelRequest();
        request.addHotelReq(params);
        hotelService.sendPrize(request);
    }
}