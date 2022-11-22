package com.hjj.flink.datastream.source.java;

import com.hjj.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-20
 * <p>
 * 自定义Source
 */
public class FlinkCustomSource {

    /**
     * 自定义source:随机数据
     * 这种自定义source还是很重要的，实际开发中可以用这种方式来生成模拟数据（根据真实数据字段来生成模拟数据），进行测试，以保证数据处理逻辑没有问题，之后再上线
     */
    class GenerateRandomDataSource implements SourceFunction<Event> {

        private Boolean runningFlag = true;

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random();
            String[] users = {"Mary", "Alice", "Bob"};
            String[] urls = {"./home", "./cart", "./fav", "./prod:?id=100", "./prod:?id=500", "./prod:?id=1000"};
            Calendar calendar = Calendar.getInstance();

            while (runningFlag) {
                String user = users[random.nextInt(users.length)];
                String url = urls[random.nextInt(urls.length)];
                sourceContext.collect(new Event(user, url, calendar.getTimeInMillis()));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            runningFlag = false;
        }
    }

    public void randomDataSource(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> stream = env.addSource(new GenerateRandomDataSource());

        stream.print();

        env.execute();
    }

    /**
     * 自定义并行source：随机数据
     */
    class GenerateRandomDataParallelSource implements ParallelSourceFunction<Event>{
        private Boolean runningFlag = true;

        @Override
        public void run(SourceContext sourceContext) throws Exception {
            Random random = new Random();
            String[] users = {"Mary", "Alice", "Bob"};
            String[] urls = {"./home", "./cart", "./fav", "./prod:?id=100", "./prod:?id=500", "./prod:?id=1000"};
            Calendar calendar = Calendar.getInstance();

            while (runningFlag) {
                String user = users[random.nextInt(users.length)];
                String url = urls[random.nextInt(urls.length)];
                sourceContext.collect(new Event(user, url, calendar.getTimeInMillis()));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            runningFlag = false;
        }
    }

    public void randomDataParallelSource(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> stream = env.addSource(new GenerateRandomDataParallelSource()).setParallelism(3);

        stream.print();

        env.execute();
    }





}
