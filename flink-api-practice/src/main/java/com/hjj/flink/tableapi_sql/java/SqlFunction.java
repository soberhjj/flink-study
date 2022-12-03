package com.hjj.flink.tableapi_sql.java;

/**
 * @author huangJunJie 2022-12-03-8:31
 *
 *在 SQL 中，我们可以把一些数据的转换操作包装起来，嵌入到 SQL 查询中统一调用，这就是"函数"（functions）
 * Flink SQL 中的函数可以分为两类：
 * 一类是 SQL 中内置的系统函数，直接通过函数名调用就可以，能够实现一些常用的转换操作，比如 COUNT()、CHAR_LENGTH()、 UPPER()等等；
 * 另一类函数是用户自定义的函数（UDF），需要在表环境中注册才能使用。
 */
public class SqlFunction {
}
