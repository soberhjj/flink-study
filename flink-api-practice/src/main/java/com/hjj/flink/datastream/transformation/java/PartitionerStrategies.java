package com.hjj.flink.datastream.transformation.java;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huangJunJie 2022-11-23-20:49
 * <p>
 * Flink的分区策略（分区器）（8种）：
 * 1.随机分区器（ShufflePartitioner）
 * 2.轮询分区器（RebalancePartitioner）
 * 3.重缩放分区器（RescalePartitioner）
 * 4.哈希分区器（KeyGroupStreamPartitioner）
 * 5.广播分区器（BroadcastPartitioner）
 * 6.全局分区器（GlobaPartitioner）
 * 7.ForwardPartitioner
 * 8.自定义分区器（CustomPartitionerWrapper）
 * <p>
 * 下面写一个自定义分区器的Demo
 */
public class PartitionerStrategies {
    public void customPartitioner(StreamExecutionEnvironment env) throws Exception {
        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);
        env.execute();
    }
}
