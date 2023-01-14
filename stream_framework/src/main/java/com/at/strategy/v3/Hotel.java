package com.at.strategy.v3;

import com.at.t1.p1.HotelRequest;
import com.at.t1.p1.HotelService;

/**
 * @create 2023-01-14
 */
public class Hotel extends AbstractStrategy implements Strategy {

    private static final Hotel instance = new Hotel();

    private HotelService hotelService;

    private Hotel() {
        register();
    }


    @Override
    public void issue(Object... params) {
        HotelRequest request = new HotelRequest();
        request.addHotelReq(params);
        hotelService.sendPrize(request);
    }
}
