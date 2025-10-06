package com.alibaba.feathub.flink.udf.bean;

/**
 * @Author: wenzhilong
 * @Email: wenzhilong@bilibili.com
 * @Date: 2025/10/06
 * @Desc:
 */
public class UserBehavior {

    private String userId;

    private String behaviorType;

    private Long value;

    private Long timestamp;

    public UserBehavior(String userId, String behaviorType, Long value, Long timestamp) {
        this.userId = userId;
        this.behaviorType = behaviorType;
        this.value = value;
        this.timestamp = timestamp;
    }

    public UserBehavior() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getBehaviorType() {
        return behaviorType;
    }

    public void setBehaviorType(String behaviorType) {
        this.behaviorType = behaviorType;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", behaviorType='" + behaviorType + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
