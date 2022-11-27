package com.hjj.flink.datastream.transformation.java;

import com.hjj.flink.datastream.source.java.FlinkCustomSource;
import com.hjj.flink.pojo.Event;
import com.hjj.flink.pojo.UrlViewCountWithWindowTime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author huangJunJie 2022-11-27-0:21
 * <p>
 * Flink的最底层API——处理函数
 * 在介绍处理函数前,无论是基本的转换、聚合，还是更为复杂的窗口操作其实都是基于DataStream进行转换的,所以可以统称为 DataStream API.
 * 在更底层,我们可以不定义任何具体的转换操作,而只是提炼出一个统一的"处理"（process）操作——它是所有转换操作的一个概括性的表达,可以自定义任何处理逻辑.处理(process)操作API就是处理函数,它是最底层的API.
 * <p>
 * 在处理函数中,我们直面的就是数据流中最基本的元素：数据事件（event）、状态（state）以及时间（time）,这就相当于对流有了完全的控制权.
 * 处理函数比较抽象,没有具体的操作,所以对于一些常见的简单应用（比如求和、开窗口）会显得有些麻烦.
 * 不过正是因为它不限定具体做什么，所以理论上我们可以做任何事情，实现所有需求。
 * 所以可以说，处理函数是我们进行 Flink 编程的“大招”，轻易不用,一旦放出来必然会扫平一切
 * <p>
 * 处理函数主要是定义数据流的转换操作
 * 处理函数的使用与DateStream API转换操作类似,只需要直接基于DataStream调用.process()方法,再把具体的处理函数传给process()方法即可.
 * <p>
 * Flink中的处理函数其实是一个大家族,我们知道DataStream在调用一些转换方法之后有可能生成新的流类型,
 * 例如调用.keyBy()之后得到 KeyedStream，进而再调用.window()之后得到 WindowedStream。
 * 对于不同类型的流，其实都可以直接调用.process()方法传入处理函数进行处理转换。Flink提供了8个不同的处理函数,它们尽管本质相同都是可以访问状态和时间信息的底层API,可彼此之间也会有所差异。
 * 1.ProcessFunction --最基本的处理函数，基于 DataStream 直接调用.process()时作为参数传入。
 * 2.KeyedProcessFunction --对流按键分区后的处理函数，基于 KeyedStream 调用.process()时作为参数传入。
 * 3.ProcessWindowFunction --开窗之后的处理函数，也是全窗口函数的代表。基于 WindowedStream 调用.process()时作为参数传入。
 * 4.ProcessAllWindowFunction --同样是开窗之后的处理函数，基于 AllWindowedStream 调用.process()时作为参数传入。
 * 5.CoProcessFunction --合并（connect）两条流之后的处理函数，基于 ConnectedStreams 调用.process()时作为参数传入。
 * 6.ProcessJoinFunction --间隔连接（interval join）两条流之后的处理函数，基于 IntervalJoined 调用.process()时作为参数传入。
 * 7.BroadcastProcessFunction --广播连接流处理函数，基于 BroadcastConnectedStream 调用.process()时作为参数传入。
 * 8.KeyedBroadcastProcessFunction --按键分区的广播连接流处理函数，同样是基于 BroadcastConnectedStream 调用.process()时作为参数传入。
 */
public class ProcessFunctions {

    /**
     * 使用处理函数实现需求：统计最近10秒钟内最热门的两个url（先求每个url访问量,再根据访问量排序取Top2）,并且每5秒更新一次
     *
     * @param env
     */
    public void topN(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());
        //设置水位线
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //1.统计每个url访问量并包装窗口的开始&结束时间
        SingleOutputStreamOperator<UrlViewCountWithWindowTime> urlCountSteam = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new WindowFunctions.UrlViewCountAggregate(), new WindowFunctions.UrlViewCountProcessWindow());

        //2.收集同一窗口时间段的 每个窗口统计出的url访问量 进行排序.(注意在上一步中每个key(也就是url)都会分配一个窗口,所以在一个窗口时间段有多少个key,就会有多少个窗口,即一个窗口对应一个key）
        SingleOutputStreamOperator<String> res = urlCountSteam.keyBy(data -> data.windowEndTime)
                .process(new TopN(2));

        res.print();

        env.execute();
    }


    //处理函数KeyedProcessFunction,实现排序取topN
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCountWithWindowTime, String> {
        //定义一个属性N,代表取top几
        private Integer N;
        //定义状态(列表类型)
        private ListState<UrlViewCountWithWindowTime> urlViewCountListState;

        public TopN(Integer N) {
            this.N = N;
        }

        @Override
        //在生命周期函数open()中获取环境中的列表状态句柄
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = this.getRuntimeContext()
                    .getListState(new ListStateDescriptor<UrlViewCountWithWindowTime>("url-count-list", Types.POJO(UrlViewCountWithWindowTime.class)));
        }

        @Override
        //数据元素处理逻辑
        public void processElement(UrlViewCountWithWindowTime value, Context ctx, Collector<String> out) throws Exception {
            //将数据保存到状态中
            urlViewCountListState.add(value);
            //注册定时器（windowEndTime + 1ms）来保证 该windowEndTime的数据全部到达
            ctx.timerService().registerEventTimeTimer(value.windowEndTime + 1);
        }

        @Override
        //定时器回调逻辑
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将列表状态变量中的数据全部取出,放入ArrayList,方便排序
            ArrayList<UrlViewCountWithWindowTime> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCountWithWindowTime urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            // 清空状态，释放资源
            urlViewCountListState.clear();

            // 排序
            urlViewCountArrayList.sort((o1, o2) -> o1.count.intValue() - o2.count.intValue());

            // 取前两名,构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + urlViewCountArrayList.get(0).windowEndTime + "\n");
            for (int i = 0; i < this.N; i++) {
                UrlViewCountWithWindowTime urlViewCountWithWindowTime = urlViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + urlViewCountWithWindowTime.url + " "
                        + "浏览量：" + urlViewCountWithWindowTime.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }


}
