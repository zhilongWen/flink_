package com.at;

import java.util.Objects;

/**
 * @create 2022-08-07
 */
public class WriteHiveTestBean {


    private long id;

    private long ts;

    private String name;

    private String address;

    public static WriteHiveTestBean of(long id, long ts, String name, String address) {
        return new WriteHiveTestBean(id, ts, name, address);
    }

    public WriteHiveTestBean() {
    }


    public WriteHiveTestBean(long id, long ts, String name, String address) {
        this.id = id;
        this.ts = ts;
        this.name = name;
        this.address = address;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WriteHiveTestBean that = (WriteHiveTestBean) o;
        return id == that.id && ts == that.ts && Objects.equals(name, that.name) && Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, name, address);
    }

    @Override
    public String toString() {
        return "WriteHiveTestBean{" +
                "id=" + id +
                ", ts=" + ts +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

}
