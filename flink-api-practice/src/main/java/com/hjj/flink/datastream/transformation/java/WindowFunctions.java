package com.hjj.flink.datastream.transformation.java;

import com.hjj.flink.datastream.source.java.FlinkCustomSource;
import com.hjj.flink.pojo.Event;
import com.hjj.flink.pojo.UrlViewCountWithWindowTime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

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
 * <p>
 * 下面练习窗口函数的使用
 */
public class WindowFunctions {
    /**
     * 增量函数：ReduceFunction（就是AggregationTransformation中的练习的reduce）
     * <p>
     * 调用reduce()
     *
     * @param env
     */
    public void reduce(StreamExecutionEnvironment env) throws Exception {
        //使用自定义数据源（数据随机生成,数据类型为Event）
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());

        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //窗口聚合（统计每个用户的访问次数,即PV）（注意每个key都会分配一个窗口而不是所有key共用一个窗口）
        SingleOutputStreamOperator<Tuple2<String, Long>> res = stream.map(element -> Tuple2.of(element.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((e1, e2) -> Tuple2.of(e1.f0, e1.f1 + e2.f1));

        res.print();

        env.execute();
    }

    /**
     * 增量函数：AggregateFunction<IN, ACC, OUT>
     * AggregateFunction可以看作是ReduceFunction的通用版本，ReduceFunction可以解决大多数的聚合问题，但是有个限制就是聚合输出&输入的数据类型必须一致,AggregateFunction突破了这个限制，可以定义更灵活的聚合操作。
     * <p>
     * 调用aggregate()
     *
     * @param env
     * @throws Exception
     */
    public void aggregate(StreamExecutionEnvironment env) throws Exception {
        //使用自定义数据源（数据随机生成,数据类型为Event）
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());

        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //窗口聚合（统计人均重复访问量,即pv/uv,也就是平均每个用户会访问多少次页面，这在一定程度上代表了用户的粘度）
        //所有数据设置相同的 key,发送到同一个分区统计 PV 和 UV,再相除
        SingleOutputStreamOperator<Double> res = stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {
                    @Override
                    //初始化累加器(累加器中的hashset类型的参数用来保存用户名实现用户名自动去重,hashset中用户数即uv,long类型的参数用来保存pv)
                    public Tuple2<HashSet<String>, Long> createAccumulator() {
                        return Tuple2.of(new HashSet<String>(), 0L);
                    }

                    @Override
                    //更新累加器
                    public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> accumulator) {
                        accumulator.f0.add(event.user);
                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
                    }

                    @Override
                    //聚合输出结果
                    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                        return (double) accumulator.f0.size() / accumulator.f1;
                    }

                    @Override
                    //合并两个累加器,即需要两个窗口合并的时候才需要使用此方法,所以在会话窗口中会使用此方法,这里没有涉及会话窗口所以merge()方法可以不做任何操作
                    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> acc2, Tuple2<HashSet<String>, Long> acc1) {
                        return null;
                    }
                });

        res.print();

        env.execute();
    }

    /**
     * 全窗口函数：WindowFunction<IN,OUT,KEY,W extends Window>
     * <p>
     * 调用apply()
     *
     * @param env
     */
    public void apply(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());
        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //窗口聚合（统计每个用户的访问次数,即PV）(相当于是上面reduce()对应的批处理版本)
        SingleOutputStreamOperator<Tuple2<String, Long>> res = stream.map(element -> Tuple2.of(element.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    /**
                     * 参数说明：
                     * String s 是key
                     * TimeWindow timeWindow 是窗口信息
                     * iterable 收集输入数据
                     * collector 收集输出数据
                     */
                    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Long pv = 0L;
                        for (Tuple2<String, Long> data : iterable) {
                            pv += 1;
                        }
                        collector.collect(Tuple2.of(s, pv));

                    }
                });

        res.print();

        env.execute();
    }


    /**
     * 全窗口函数：ProcessWindowFunction<IN,OUT,KEY,W extends Window>
     * ProcessWindowFunction相比于WindowFunction的增强版.
     * WindowFunction能提供的上下文信息较少(只能获取窗口信息),也没有更高级的功能. ProcessWindowFunction能够完全覆盖WindowFunction,并能获取上下文信息(如处理时间、状态).
     * 在实际开发中直接使用ProcessWindowFunction即可.
     * 事实上,ProcessWindowFunction是Flink底层API——处理函数（process function）中的一员.
     * <p>
     * 调用process()
     *
     * @param env
     */
    public void process(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());
        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //窗口聚合（统计每个用户的访问次数,即PV）（相当于上面WindowFunction的增强版，除了窗口信息外还能获取处理时间、状态等更多上下文信息）
        SingleOutputStreamOperator<Tuple2<String, Long>> res = stream.map(element -> Tuple2.of(element.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    /**
                     * 参数说明：
                     * String s 是key
                     * Context context 是上下文信息
                     * iterable 收集输入数据
                     * collector 收集输出数据
                     */
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Long pv = 0L;
                        for (Tuple2<String, Long> data : iterable) {
                            pv += 1;
                        }
                        collector.collect(Tuple2.of("处理时间-" + context.currentProcessingTime() + ":" + s, pv));
                    }
                });

        res.print();

        env.execute();
    }

    /**
     * 增量函数和全窗口函数结合使用
     * <p>
     * 为什么结合使用？因为增量函数是无法获取运行上下文信息的,而全窗口函数可以获取运行上下文信息,
     * 结合使用用用增量函数处理数据，然后增量函数的计算结果并不是直接输出,而是传递给全窗口函数,
     * 可想而知全窗口函数只会拿到一条数据,就是增量函数的计算结果数据,然后全窗口函数中去拿到运行时上下文信息和计算结果数据一同输出,
     * 这样也就是相当于结合了增量函数和全窗口函数的各自的优点,即增量函数更为高效的数据聚合(流处理方式)+全窗口函数能拿到运行时上下文信息。
     * <p>
     * 下面写一个增量函数AggregateFunction和全窗口函数ProcessWindowFunction结合使用Demo,
     * 需求是统计10秒钟的url点击量,同时为了更加清晰地展示还应该把窗口的起始结束时间一起输出
     *
     * @param env
     */
    public void aggregateAndProcess(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());
        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //按照url分组,开滑动窗口统计
        SingleOutputStreamOperator<UrlViewCountWithWindowTime> res = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAggregate(), new UrlViewCountProcessWindow());

        res.map(data -> data.toString()).print();

        env.execute();
    }

    //增量聚合函数,统计url点击量
    public static class UrlViewCountAggregate implements AggregateFunction<Event, Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        //初始化累加器
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of(null, 0L);
        }

        @Override
        //更新累加器
        public Tuple2<String, Long> add(Event event, Tuple2<String, Long> acc) {
            return Tuple2.of(event.url, acc.f1 + 1);
        }

        @Override
        //聚合输出结果
        public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {
            return Tuple2.of(acc.f0, acc.f1);
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
            return null;
        }
    }

    //全窗口函数,只需要包装窗口信息
    public static class UrlViewCountProcessWindow extends ProcessWindowFunction<Tuple2<String, Long>, UrlViewCountWithWindowTime, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<UrlViewCountWithWindowTime> collector) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Tuple2<String, Long> urlViewCount = iterable.iterator().next();
            collector.collect(new UrlViewCountWithWindowTime(urlViewCount.f0, urlViewCount.f1, start, end));
        }
    }


}
