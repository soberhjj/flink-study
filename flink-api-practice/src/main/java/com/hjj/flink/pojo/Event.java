package com.hjj.flink.pojo;

import java.sql.Timestamp;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-20
 */
public class Event {
    public String User; //属性直接设为public,省的加getter/setter
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        User = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "User='" + User + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
