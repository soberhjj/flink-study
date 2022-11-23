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

        BasicTransformation basicTransformation = new BasicTransformation();
        AggregationTransformation aggregationTransformation = new AggregationTransformation();
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
            default:
                System.out.println("sorry, no this source type");
        }
    }
}
