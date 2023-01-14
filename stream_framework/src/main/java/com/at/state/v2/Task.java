package com.at.state.v2;

/**
 * @create 2023-01-14
 */
public class Task {
    private Long taskId;
    // 初始化为初始态
    private State state = new TaskInit();
    // 更新状态
    public void updateState(ActionType actionType) {
        state.update(this, actionType);
    }

    public void setState(State state) {

    }
}
