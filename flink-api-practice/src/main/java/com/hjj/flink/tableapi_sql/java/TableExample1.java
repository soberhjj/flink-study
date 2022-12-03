package com.hjj.flink.tableapi_sql.java;

import com.hjj.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author huangJunJie 2022-12-03-7:21
 * <p>
 * 简单示例
 */
public class TableExample1 {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 用执行 SQL 的方式提取数据 (注意timestamp是flink的保留关键字,强行使用需要加``)
        Table visitTable = tableEnv.sqlQuery("select user,url,`timestamp` from " + eventTable);
        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable).print();

        //用 Table API 方式提取数据
        Table visitTable2 = eventTable.select($("user"), $("url"));
        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable2).print();

        // 执行程序
        env.execute();
    }
}

