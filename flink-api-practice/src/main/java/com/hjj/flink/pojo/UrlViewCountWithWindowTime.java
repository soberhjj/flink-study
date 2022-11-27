package com.hjj.flink.pojo;

/**
 * @author huangJunJie 2022-11-27-21:20
 */
public class UrlViewCountWithWindowTime {
    public String url;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public UrlViewCountWithWindowTime() {
    }

    public UrlViewCountWithWindowTime(String url, Long count, Long windowStartTime, Long windowEndTime) {
        this.url = url;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "UrlViewCountWithWindowTime{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStartTime=" + windowStartTime +
                ", windowEndTime=" + windowEndTime +
                '}';
    }
}
