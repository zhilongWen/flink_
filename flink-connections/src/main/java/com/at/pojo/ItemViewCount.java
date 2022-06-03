package com.at.pojo;

import lombok.*;

import java.util.Objects;

/**
 * @create 2022-06-03
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ItemViewCount {

    private Long itemId;
    private Long windowStart;
    private Long windowEnd;
    private Long count;

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowSnd=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemViewCount that = (ItemViewCount) o;
        return Objects.equals(itemId, that.itemId) && Objects.equals(windowStart, that.windowStart) && Objects.equals(windowEnd, that.windowEnd) && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, windowStart, windowEnd, count);
    }
}
