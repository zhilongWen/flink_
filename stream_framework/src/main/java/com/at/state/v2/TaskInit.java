package com.at.state.v2;


/**
 * @create 2023-01-14
 */
public class TaskInit implements State {
    @Override
    public void update(Task task, ActionType actionType) {
        if  (actionType == ActionType.START) {
            task.setState(new TaskOngoing());
        }
    }

}
