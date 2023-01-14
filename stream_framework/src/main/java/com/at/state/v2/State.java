package com.at.state.v2;


/**
 * @create 2023-01-14
 */
public interface State {
    // 默认实现，不做任何处理
    default void update(Task task, ActionType actionType) {
        // do nothing
    }
}
