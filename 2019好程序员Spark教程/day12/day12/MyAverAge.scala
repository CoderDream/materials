package com.qf.gp1922.day12

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

/**
  * 实现UDAF聚合函数, 因为操作的是DataFrame，弱类型的，不用指定具体的类型
  */
class MyAverAge extends UserDefinedAggregateFunction{
  /**
    * 指定输入数据类型
    * @return
    */
  override def inputSchema: StructType = StructType(Array(StructField("age", IntegerType, true)))

  /**
    * 指定每个分区存储的数据的类型
    * sum: 年龄的总和
    * count：年龄的个数
    * @return
    */
  override def bufferSchema: StructType = StructType(StructField("sum", IntegerType) :: StructField("count", IntegerType) :: Nil)

  /**
    * 返回数据类型
    * @return
    */
  override def dataType: DataType = IntegerType

  /**
    * 如果为true：有相同的输入，就有相同的输出
    * UDAF函数中如果输入的数据掺杂着时间,不同时间得到的结果可能是不一样的所以这个值可以设置为false
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 初始化的方法
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 给buffer定义初始值, 定义年龄的总和
    buffer(0) = 0 // sum
    // 给buffer定义初始值, 定义年龄的个数
    buffer(1) = 0 // count

  }

  /**
    * 局部聚合的方法
    * update方法操作的就是一个分区的数据
    * @param buffer 上面那个buffer，指的是用于计算的共享变量
    * @param input 传进来的将要聚合的数据
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getInt(0) + input.getInt(0)
      buffer(1) = buffer.getInt(1) + 1
    }
  }

  /**
    * 全局聚合的方法
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0) // 年龄总数
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1) // 参与计算的人数
  }

  /**
    * 最终输出结果方法，可以在该方法中影响最终输出结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getInt(0) / buffer.getInt(1)
}
