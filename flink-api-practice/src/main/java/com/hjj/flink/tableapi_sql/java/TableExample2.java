package com.hjj.flink.tableapi_sql.java;

import com.hjj.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author huangJunJie 2022-12-03-7:42
 * <p>
 * 综合应用示例：
 * 同样还是用户的一组点击事件，查询出某个用户（例如 Alice）点击的 url 列表，并统计出每个用户累计的点击次数，
 */
public class TableExample2 {
    public static void main(String[] args) throws Exception {
        // 获取流环境
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
        tableEnv.createTemporaryView("EventTable", eventStream);

        // 查询 Alice 的访问 url 列表
        Table aliceVisitTable = tableEnv.sqlQuery("SELECT url, user FROM EventTable WHERE user = 'Alice'");

        // 统计每个用户的点击次数
        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) FROM EventTable GROUP BY user");

        // 将表转换成数据流，在控制台打印输出
        tableEnv.toDataStream(aliceVisitTable).print("alice visit");
        tableEnv.toChangelogStream(urlCountTable).print("count");

        // 执行程序
        env.execute();
    }
}
