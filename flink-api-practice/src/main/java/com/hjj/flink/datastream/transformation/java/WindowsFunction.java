package com.hjj.flink.datastream.transformation.java;

import com.hjj.flink.datastream.source.java.FlinkCustomSource;
import com.hjj.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.scala.typeutils.Types$;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author huangJunJie 2022-11-24-17:58
 * <p>
 * 窗口是从无界数据流中截取有界数据的一种方式；
 * 窗口截取数据的方式（即窗口是以什么标准来开始和结束数据的截取）我们把它叫做窗口的"驱动类型"
 * 根据窗口驱动类型的不同，可将窗口分为：
 * 1.时间窗口（Time Window）:由时间驱动，按时间段截取数据
 * 2.计数窗口（Count Window）:由元素个数驱动，按元素的个数截取数据
 * <p>
 * 时间窗口和计数窗口只是根据窗口驱动类型对窗口的一个大致划分，在具体应用时，还需要定义更加精细的规则来控制数据应该划分到哪个窗口中去。
 * 不同的分配数据的方式，就可以有不同的功能应用。根据分配数据的规则，窗口的具体实现可以分为 4 类：
 * 1.滚动窗口（Tumbling Window）、
 * 2.滑动窗口（Sliding Window）、
 * 3.会话窗口（Session Window）、
 * 4.全局窗口（Global Window）
 * <p>
 * 窗口函数根据对窗口中数据处理方式的不同(流处理方式与批处理方式)，分为两类：增量聚合函数(流处理方式)和全窗口函数(批处理方式)
 */
public class WindowsFunction {
    /**
     * 增量聚合：reduce()
     * @param env
     */
    public void reduce(StreamExecutionEnvironment env) throws Exception {
        //使用自定义数据源（数据随机生成,数据类型为Event）
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());

        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //窗口聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> res = stream.map(element -> Tuple2.of(element.User, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((e1, e2) -> Tuple2.of(e1.f0, e1.f1 + e2.f1));

        res.print();

        env.execute();
    }


}
