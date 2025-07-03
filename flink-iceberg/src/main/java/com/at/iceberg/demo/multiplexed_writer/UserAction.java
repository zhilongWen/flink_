package com.at.iceberg.demo.multiplexed_writer;

// 用户行为POJO
public class UserAction {
    public int user_id;
    public String action;
    public long timestamp;
    public String details;

    public UserAction(int user_id, String action, long timestamp, String details) {
        this.user_id = user_id;
        this.action = action;
        this.timestamp = timestamp;
        this.details = details;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "user_id=" + user_id +
                ", action='" + action + '\'' +
                ", timestamp=" + timestamp +
                ", details='" + details + '\'' +
                '}';
    }
}
