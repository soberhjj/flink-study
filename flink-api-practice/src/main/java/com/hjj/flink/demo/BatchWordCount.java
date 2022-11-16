package com.hjj.flink.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author huangJunJie 2022-11-15-20:02
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境, Flink 在执行应用程序前应该获取执行环境对象，也就是运行时上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 数据源(本地文本文件)
        DataSource<String> lineDS = env.readTextFile("E:\\IDEA\\Projects\\flink-study\\flink-api-practice\\src\\main\\resources\\test_data\\words.txt");
        // 3. 数据处理
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));////当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        //4.数据输出(打印到控制台)
        sum.print();
    }
}
