package com.hjj.flink.datastream.transformation.java;

import com.hjj.flink.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huangJunJie 2022-11-23-20:26
 * <p>
 * 富函数类（Rich Function）
 * 富函数类同普通函数类一样也是DataStream API提供的一个函数类的接口，所有的Flink函数类都有其Rich版本。
 * 富函数类与普通函数类不同主要在于其可以获取运行环境上下文（如程序执行的并行度，task名称，以及状态（state）），并拥有一些生命周期方法（典型的open()方法和close()方法）。
 * 因此富函数可以实现更复杂的功能
 * <p>
 * 下面写一个demo：使用富函数转换数据，并在task开始处理数据前&数据处理结束后 使用生命周期方法打印task名称，因此在生命周期方法中要通过运行环境上下文拿到task名称
 */
public class RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        //将点击事件转换成长整型的时间戳输出
        SingleOutputStreamOperator<Long> richMap = clicks.map(new RichMapFunction<Event, Long>() {
            //生命周期方法open()
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("task-" + getRuntimeContext().getIndexOfThisSubtask() + " 开始执行");
            }

            @Override
            public Long map(Event event) throws Exception {
                return event.timestamp;
            }

            //生命周期方法close()
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("task-" + getRuntimeContext().getIndexOfThisSubtask() + " 执行结束");
            }
        });

        richMap.print();

        env.execute();
    }

    /**
     * 一个常见的应用场景就是，如果我们希望连接到一个外部数据库进行读写操作，那么将连
     * 接操作放在 map()中显然不是个好选择——因为每来一条数据就会重新连接一次数据库；所以
     * 我们可以在 open()中建立连接，在 map()中读写数据，而在 close()中关闭连接。
     */
}
