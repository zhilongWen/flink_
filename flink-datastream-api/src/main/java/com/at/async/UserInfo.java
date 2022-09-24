package com.at.async;


import java.util.Objects;

/**
 * @create 2022-05-31
 */
public class UserInfo {


    private String name;
    private Integer age;
    private Integer sex;
    private String address;


    public UserInfo() {
    }

    public UserInfo(String name, Integer age, Integer sex, String address) {
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.address = address;
    }

    public static UserInfo of(String name, Integer age, Integer sex, String address){
        return new UserInfo(name,age,sex,address);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        return "UserInfo{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", sex=" + sex +
                ", address='" + address + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserInfo userInfo = (UserInfo) o;
        return Objects.equals(name, userInfo.name) && Objects.equals(age, userInfo.age) && Objects.equals(sex, userInfo.sex) && Objects.equals(address, userInfo.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age, sex, address);
    }
}
