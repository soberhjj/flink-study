package com.hjj.flink.datastream.source.java;

import com.hjj.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-20
 * <p>
 * 练习Flink的所有内置source，从不同source读取数据，不做转换&计算处理，直接输出到控制台
 */
public class FlinkInnerSource {
    /**
     * 从集合读取数据
     * @param env
     * @throws Exception
     */
    public void collectionSource(StreamExecutionEnvironment env) throws Exception {
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));

        //代码中直接创建一个集合，以该集合作为数据源，一般用于测试
        DataStream<Event> stream = env.fromCollection(clicks);

        /**
         * 也可以不构建集合，直接将元素列举出来，调用 fromElements 方法进行读取数据，如下
         */
//        DataStreamSource<Event> stream2 = env.fromElements(
//                new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L)
//        );

        stream.print();

        env.execute();
    }

    /**
     * 从文件（本地文件、分布式文件系统）读取数据
     * @param env
     * @throws Exception
     */
    public void fileSource(StreamExecutionEnvironment env) throws Exception {
        DataStream<String> stream = env.readTextFile("flink-api-practice/src/main/resources/input_data/clicks.txt");

        stream.print();

        env.execute();
    }

    /**
     * 从socket读取数据
     * @param env
     * @throws Exception
     */
    public void socketSource(StreamExecutionEnvironment env) throws Exception {
        DataStream<String> stream = env.socketTextStream("192.168.204.101", 7777);

        stream.print();

        env.execute();
    }


}
