package com.at.async;

import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;

/**
 * @create 2022-05-31
 */
public class ClickUserInfo {

    private String name;
    private String url;
    private Long timestamp;

    private Integer age;
    private Integer sex;
    private String address;

    public ClickUserInfo() {
    }

    public ClickUserInfo(String name, String url, Long timestamp, Integer age, Integer sex, String address) {
        this.name = name;
        this.url = url;
        this.timestamp = timestamp;
        this.age = age;
        this.sex = sex;
        this.address = address;
    }

    public static ClickUserInfo of(String name, String url, Long timestamp, Integer age, Integer sex, String address) {
        return new ClickUserInfo(name, url, timestamp, age, sex, address);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "ClickUserInfo{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                ", age=" + age +
                ", sex=" + Optional.ofNullable(sex).map(r -> r.intValue() == 1 ? "男" : "女").get() +
                ", address='" + address + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClickUserInfo that = (ClickUserInfo) o;
        return Objects.equals(name, that.name) && Objects.equals(url, that.url) && Objects.equals(timestamp, that.timestamp) && Objects.equals(age, that.age) && Objects.equals(sex, that.sex) && Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, url, timestamp, age, sex, address);
    }
}
