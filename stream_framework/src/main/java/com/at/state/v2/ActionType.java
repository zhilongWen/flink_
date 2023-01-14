package com.at.state.v2;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @create 2023-01-14
 */
@AllArgsConstructor
@Getter
public enum ActionType {
    START(1, "开始"),
    STOP(2, "暂停"),
    ACHIEVE(3, "完成"),
    EXPIRE(4, "过期")
    ;
    private final int code;
    private final String message;
}
