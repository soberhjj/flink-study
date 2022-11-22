package com.hjj.flink.datastream.source.java;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: Huang JunJie
 * @CreateTime: 2022-11-20
 * <p>
 * 练习Flink的第三方source，从不同source读取数据，不做转换&计算处理，直接输出到控制台
 */
public class FlinkThirdSource {
    /**
     * 从kafka读取数据
     *
     * @param env
     * @throws Exception
     */
    public void kafkaSource(StreamExecutionEnvironment env) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.204.101:9092,192.168.204.102:9092,192.168.204.103:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("flink-api-practice-clicks", new SimpleStringSchema(), properties));

        stream.print();

        env.execute();
    }
}
