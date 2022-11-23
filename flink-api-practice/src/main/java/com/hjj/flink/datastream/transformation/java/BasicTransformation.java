package com.hjj.flink.datastream.transformation.java;

import com.hjj.flink.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-23
 * <p>
 * 练习基本转换算子，直接从内存集合读取数据，进行数据转换处理，直接输出到控制台
 */
public class BasicTransformation {

    /**
     * map算子
     *
     * @param env
     */
    public void map(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        /**
         * 1.匿名类写法
         */
//        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<Event, String>() {
//            @Override
//            public String map(Event event) throws Exception {
//                return event.User;
//            }
//        });

        /**
         * 2.lambda表达式写法
         */
        SingleOutputStreamOperator<String> map = stream.map(data -> data.User);

        /**
         * 3.MapFunction实现类写法
         */
//        SingleOutputStreamOperator<String> map = stream.map(new UserExtractor());

        map.print();

        env.execute();
    }

    class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.User;
        }
    }


    /**
     * filter算子
     *
     * @param env
     * @throws Exception
     */
    public void filter(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./cart", 3000L),
                new Event("Mary", "./cart", 4000L),
                new Event("Bob", "./cart", 5000L),
                new Event("Bob", "./cart", 6000L)
        );

        SingleOutputStreamOperator<Event> filter = stream.filter(data -> data.User.equals("Bob"));

        filter.print();

        env.execute();
    }


    /**
     * flatMap算子
     * @param env
     * @throws Exception
     */
    public void flatMap(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary,Cindy", "./home", 1000L),
                new Event("Bob,Alice", "./cart", 2000L),
                new Event("Alice,Mary", "./cart", 3000L),
                new Event("Mary", "./cart", 4000L),
                new Event("Bob,Cindy", "./cart", 5000L),
                new Event("Bob", "./cart", 6000L)
        );

        SingleOutputStreamOperator<String> flatMap = stream.flatMap((Event event, Collector<String> out) -> {
//            String[] users = event.User.split(",");
//            for (String user : users) {
//                out.collect(user);
//            }

            //简洁写法
            Arrays.stream(event.User.split(",")).forEach(out::collect);
        }).returns(Types.STRING);

        flatMap.print();

        env.execute();
    }


}
