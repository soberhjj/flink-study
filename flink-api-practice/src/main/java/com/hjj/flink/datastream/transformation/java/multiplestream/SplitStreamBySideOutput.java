package com.hjj.flink.datastream.transformation.java.multiplestream;

import com.hjj.flink.datastream.source.java.FlinkCustomSource;
import com.hjj.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author huangJunJie 2022-11-28-8:12
 * <p>
 * 侧输出流：使用侧输出流进行分流
 * <p>
 * 侧输出流的输出数据的类型可以与原本的流的数据类型不一致
 */
public class SplitStreamBySideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new FlinkCustomSource.GenerateRandomDataSource());

        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        //使用使用侧输出流进行分流
        //定义输出标签
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {
        };
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Bob") {
        };
        //侧输出流分流
        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")) {
                    ctx.output(maryTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")) {
                    ctx.output(bobTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });

        processStream.print("主流");
        processStream.getSideOutput(maryTag).print("侧输出流:Mary");
        processStream.getSideOutput(bobTag).print("侧输出流:Bob");

        env.execute();
    }
}
