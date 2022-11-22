package com.hjj.flink.datastream.source.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-20
 *
 * 启动类
 */
public class Startor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        FlinkInnerSource flinkInnerSource = new FlinkInnerSource();
        FlinkThirdSource flinkThirdSource = new FlinkThirdSource();
        FlinkCustomSource flinkCustomSource = new FlinkCustomSource();

        String sourceType = args[0];
        switch (sourceType) {
            case "colletion":
                flinkInnerSource.collectionSource(env);
                break;
            case "file":
                flinkInnerSource.fileSource(env);
                break;
            case "socket":
                flinkInnerSource.socketSource(env);
                break;
            case "kafka":
                flinkThirdSource.kafkaSource(env);
                break;
            case "custom_random_data":
                flinkCustomSource.randomDataSource(env);
                break;
            case "custom_parallel_random_data":
                flinkCustomSource.randomDataParallelSource(env);
                break;
            default:
                System.out.println("sorry, no this source type");
        }
    }
}
