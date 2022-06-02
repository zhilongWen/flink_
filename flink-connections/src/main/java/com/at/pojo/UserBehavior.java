package com.at.pojo;

import lombok.*;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * @create 2022-06-01
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class UserBehavior {

    //用户ID
    public int userId;
    //商品ID
    public long itemId;
    //商品类目ID
    public  int categoryId;
    //用户行为，包括{"pv","buy","cart", "fav"}
    public String behavior;
    //行为发生的时间戳，单位秒
    public long ts;

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
