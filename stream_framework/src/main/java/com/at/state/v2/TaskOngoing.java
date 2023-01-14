package com.at.state.v2;


/**
 * @create 2023-01-14
 */
public class TaskOngoing implements State {

    private ActivityService activityService;
    private TaskManager taskManager;



    @Override
    public void update(Task task, ActionType actionType) {
        if (actionType == ActionType.ACHIEVE) {
            task.setState(new TaskFinished());
            // 通知
            activityService.notifyFinished(task);
            taskManager.release(task);
        } else if (actionType == ActionType.STOP) {
            task.setState(new TaskPaused());
        } else if (actionType == ActionType.EXPIRE) {
            task.setState(new TaskExpired());
        }
    }
}
