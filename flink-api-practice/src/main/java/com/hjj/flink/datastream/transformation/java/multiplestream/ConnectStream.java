package com.hjj.flink.datastream.transformation.java.multiplestream;

/**
 * @author huangJunJie 2022-11-28-9:28
 *
 * 连接流（connect）
 *
 * 联合流(union)受限于数据类型不能改变,灵活性大打折扣,所以实际应用较少出现
 * 除了联合（union），Flink 还提供了另外一种方便的合流操作——连接（connect）
 *
 * 为了处理更加灵活，连接操作允许流的数据类型不同。但我们知道一个 DataStream 中的数据只能有唯一的类型，
 * 所以连接得到的并不是 DataStream，而是一个"连接流"（ConnectedStreams）。
 * 连接流可以看成是两条流形式上的"统一"，被放在了一个同一个流中；事实上内部仍保持各自的数据形式不变，彼此之间是相互独立的。
 * 要想得到新的 DataStream，还需要进一步定义一个“同处理”（co-process）转换操作，用来说明对于不同来源、不同类型的数据，怎样分别进行处理转换、得到统一的输出类型。
 *
 * 注意联合流(union)没有流的数量限制，而连接流(connect)流的数量只能是2
 */
public class ConnectStream {
}
