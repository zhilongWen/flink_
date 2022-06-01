package com.at.async;

import com.at.pojo.Event;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * @create 2022-05-31
 */
public interface ConnectFunction {

    String getKey(Event event);

   ClickUserInfo join(Event input, UserInfo userInfo);

}
