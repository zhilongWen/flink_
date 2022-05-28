package com.at.pojo;

import java.sql.Timestamp;

/**
 * @create 2022-05-15
 */
public class Event {

    public String user;
    public String url;
    public Long timestamp;


    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }



}
