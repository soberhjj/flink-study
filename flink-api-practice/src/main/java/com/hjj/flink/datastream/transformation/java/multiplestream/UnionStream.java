package com.hjj.flink.datastream.transformation.java.multiplestream;

import com.hjj.flink.datastream.source.java.FlinkCustomSource;
import com.hjj.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author huangJunJie 2022-11-28-8:38
 * <p>
 * 联合流(union)
 * 联合操作要求必须流中的数据类型相同，合并之后的新流会包括所有流中的元素且数据类型不变
 * <p>
 * 这里需要考虑一个问题。在事件时间语义下，水位线是时间的进度标志；
 * 不同的流中可能水位线的进展快慢完全不同，如果它们合并在一起，水位线又该以哪个为准呢？
 * 答案以最小的那个水位线为准，下面就来测试一下
 */
public class UnionStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("192.168.204.101", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        stream1.print("stream1");

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("192.168.204.102", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        stream2.print("stream2");

        //合并两条流
        SingleOutputStreamOperator<String> unionStream = stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("watermark:" + ctx.timerService().currentWatermark());
                    }
                });

        unionStream.print();

        env.execute();
    }
}
