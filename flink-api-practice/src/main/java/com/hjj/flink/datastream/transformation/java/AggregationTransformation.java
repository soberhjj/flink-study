package com.hjj.flink.datastream.transformation.java;

import com.hjj.flink.datastream.source.java.FlinkCustomSource;
import com.hjj.flink.pojo.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-23
 * <p>
 * 练习聚合算子，直接从内存集合读取数据，进行数据聚计算处理，直接输出到控制台
 * <p>
 * 关于keyBy()：
 * 对于Flink而言，DataStream 是没有直接进行聚合的 API 的，要做聚合需要先对数据进行按键（key）分组，具有相同key的数据会被发往下游的同一个分区，
 * 只有keyBy之后才能进行聚合计算，注意如果key是POJO的话必须要重写hashCode方法，因为keyBy内部是通过key的哈希值对下游分区数取模来决定数据发往下游的哪一个分区的（即哈希分区）
 * keyBy并不是一个转换算子，它只是将DataStream 转换为KeyedStream（键控流），只有基于KeyedStream才能进行聚合操作（比如sum、reduce），
 * 而且KeyedStream也会将当前算子任务的状态（state）按照key进行划分、限定为仅对当前key有效。
 * <p>
 * 注意聚合算子操作前后流中的元素的数据类型不会发生改变！
 */
public class AggregationTransformation {

    /**
     * 简单聚合：sum()、min()、max()、minBy()、maxBy()
     * <p>
     * min()和minBy()的区别：
     * min()返回的数据记录是 指定参数field的最小值 + 其他field的原来的值
     * minBy()返回的数据记录是 指定参数field为最小值的那整条记录
     * <p>
     * max()与maxBy同理
     */
    public void simpleAggregation(StreamExecutionEnvironment env) throws Exception {
        //对元组数据流进行聚合
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        //对于元祖数据，既可以通过元素位置，也可以通过元素字段名称，来指定聚合字段
        stream.keyBy(r -> r.f0).sum(1).print();
        stream.keyBy(r -> r.f0).sum("f1").print();
        stream.keyBy(r -> r.f0).max(1).print();
        stream.keyBy(r -> r.f0).max("f1").print();
        stream.keyBy(r -> r.f0).min(1).print();
        stream.keyBy(r -> r.f0).min("f1").print();
        stream.keyBy(r -> r.f0).maxBy(1).print();
        stream.keyBy(r -> r.f0).maxBy("f1").print();
        stream.keyBy(r -> r.f0).minBy(1).print();
        stream.keyBy(r -> r.f0).minBy("f1").print();

        //对POJO类型的数据流进行聚合，不能通过位置，只能通过字段名称来指定聚合字段
        DataStreamSource<Event> streamPojo = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        streamPojo.keyBy(e -> e.user).max("timestamp").print();

        env.execute();
    }

    /**
     * 归约聚合：reduce()
     * <p>
     * 上面的简单聚合是对一些特定统计需求的实现，而reduce()是一个一般化的聚合算子，可以根据自己的需求开发对应的聚合逻辑
     * <p>
     * 下面用reduce来实现一个例子：获取所有访问用户中访问频次最高的那个，也就是当前访问量最大的用户是谁
     */
    public void reduce(StreamExecutionEnvironment env) throws Exception {
        //使用自定义数据源（数据随机生成,数据类型为Event）
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());

        //先用一个reduce算子实现sum的功能,再用一个reduce算子实现maxBy的功能
        SingleOutputStreamOperator<Tuple2<String, Long>> res = source.map(e -> Tuple2.of(e.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .reduce((e1, e2) -> Tuple2.of(e1.f0, e1.f1 + e2.f1))
                .keyBy(r -> true)
                .reduce((e1, e2) -> e1.f1 > e2.f1 ? e1 : e2);

        res.print();

        env.execute();
    }
}
