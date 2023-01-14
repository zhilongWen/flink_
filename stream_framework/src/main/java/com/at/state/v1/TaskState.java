package com.at.state.v1;


import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @create 2023-01-14
 */
// 任务状态枚举
@AllArgsConstructor
@Getter
public enum TaskState {
    INIT("初始化"),
    ONGOING( "进行中"),
    PAUSED("暂停中"),
    FINISHED("已完成"),
    EXPIRED("已过期")
    ;
    private final String message;

}
