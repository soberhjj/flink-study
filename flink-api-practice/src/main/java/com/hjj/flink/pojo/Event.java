package com.hjj.flink.pojo;

import java.sql.Timestamp;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-20
 */
public class Event {
    public String user; //属性直接设为public,省的加getter/setter
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
