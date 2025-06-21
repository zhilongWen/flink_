package com.at.iceberg.demo;


import java.sql.Timestamp;
import java.util.Objects;


public class UserBehavior {

    //用户ID
    public int userId;
    //商品ID
    public long itemId;
    //商品类目ID
    public int categoryId;
    //用户行为，包括{"pv","buy","cart", "fav"}
    public String behavior;
    //行为发生的时间戳，单位秒
    public long ts;

    public UserBehavior() {
    }

    public UserBehavior(int userId, long itemId, int categoryId, String behavior, long ts) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.ts = ts;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserBehavior that = (UserBehavior) o;
        return userId == that.userId && itemId == that.itemId && categoryId == that.categoryId && ts == that.ts && Objects.equals(behavior, that.behavior);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, itemId, categoryId, behavior, ts);
    }
}
