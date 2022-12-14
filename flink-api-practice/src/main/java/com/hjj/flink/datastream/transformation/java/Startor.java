package com.hjj.flink.datastream.transformation.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-23
 *
 * 启动类
 */
public class Startor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //基础转换
        BasicTransformation basicTransformation = new BasicTransformation();
        //聚合
        AggregationTransformation aggregationTransformation = new AggregationTransformation();
        //分区器
        PartitionerStrategies partitionerStrategies = new PartitionerStrategies();
        //转换&聚合的富函数版本
        RichFunction richFunction = new RichFunction();
        //窗口函数
        WindowFunctions windowFunctions = new WindowFunctions();
        //处理函数
        ProcessFunctions processFunctions = new ProcessFunctions();

        String sourceType = args[0];
        switch (sourceType) {
            case "map":
                basicTransformation.map(env);
                break;
            case "filter":
                basicTransformation.filter(env);
                break;
            case "flatMap":
                basicTransformation.flatMap(env);
                break;
            case "simpleAggregation":
                aggregationTransformation.simpleAggregation(env);
                break;
            case "reduce":
                aggregationTransformation.reduce(env);
                break;
            case "custom-partitioner":
                partitionerStrategies.customPartitioner(env);
                break;
            case "rich-map":
                richFunction.richMap(env);
                break;
            case "window-reduce":
                windowFunctions.reduce(env);
                break;
            case "window-aggregate":
                windowFunctions.aggregate(env);
                break;
            case "window-apply":
                windowFunctions.apply(env);
                break;
            case "window-process":
                windowFunctions.process(env);
                break;
            case "window-aggregate-process":
                windowFunctions.aggregateAndProcess(env);
                break;
            case "process_function-topN":
                processFunctions.topN(env);
                break;
            default:
                System.out.println("sorry, no this source type");
        }
    }
}
