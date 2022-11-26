package com.hjj.flink.datastream.transformation.java;

/**
 * @author huangJunJie 2022-11-27-0:21
 * <p>
 * Flink的最底层API——处理函数
 * 在介绍处理函数前,无论是基本的转换、聚合，还是更为复杂的窗口操作其实都是基于DataStream进行转换的,所以可以统称为 DataStream API.
 * 在更底层,我们可以不定义任何具体的转换操作,而只是提炼出一个统一的"处理"（process）操作——它是所有转换操作的一个概括性的表达,可以自定义任何处理逻辑.处理(process)操作API就是处理函数,它是最底层的API.
 * <p>
 * 在处理函数中,我们直面的就是数据流中最基本的元素：数据事件（event）、状态（state）以及时间（time）,这就相当于对流有了完全的控制权.
 * 处理函数比较抽象,没有具体的操作,所以对于一些常见的简单应用（比如求和、开窗口）会显得有些麻烦.
 * 不过正是因为它不限定具体做什么，所以理论上我们可以做任何事情，实现所有需求。
 * 所以可以说，处理函数是我们进行 Flink 编程的“大招”，轻易不用,一旦放出来必然会扫平一切
 * <p>
 * 处理函数主要是定义数据流的转换操作
 * 处理函数的使用与DateStream API转换操作类似,只需要直接基于DataStream调用.process()方法,再把具体的处理函数传给process()方法即可.
 * <p>
 * Flink中的处理函数其实是一个大家族,我们知道DataStream在调用一些转换方法之后有可能生成新的流类型,
 * 例如调用.keyBy()之后得到 KeyedStream，进而再调用.window()之后得到 WindowedStream。
 * 对于不同类型的流，其实都可以直接调用.process()方法传入处理函数进行处理转换。Flink提供了8个不同的处理函数,它们尽管本质相同都是可以访问状态和时间信息的底层API,可彼此之间也会有所差异。
 * 1.ProcessFunction --最基本的处理函数，基于 DataStream 直接调用.process()时作为参数传入。
 * 2.KeyedProcessFunction --对流按键分区后的处理函数，基于 KeyedStream 调用.process()时作为参数传入。
 * 3.ProcessWindowFunction --开窗之后的处理函数，也是全窗口函数的代表。基于 WindowedStream 调用.process()时作为参数传入。
 * 4.ProcessAllWindowFunction --同样是开窗之后的处理函数，基于 AllWindowedStream 调用.process()时作为参数传入。
 * 5.CoProcessFunction --合并（connect）两条流之后的处理函数，基于 ConnectedStreams 调用.process()时作为参数传入。
 * 6.ProcessJoinFunction --间隔连接（interval join）两条流之后的处理函数，基于 IntervalJoined 调用.process()时作为参数传入。
 * 7.BroadcastProcessFunction --广播连接流处理函数，基于 BroadcastConnectedStream 调用.process()时作为参数传入。
 * 8.KeyedBroadcastProcessFunction --按键分区的广播连接流处理函数，同样是基于 BroadcastConnectedStream 调用.process()时作为参数传入。
 */
public class ProcessFunction {


}
