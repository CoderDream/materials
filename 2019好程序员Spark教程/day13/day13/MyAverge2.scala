package com.qf.gp1922.day13

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * Aggregator[-IN, BUF, OUT]
  * 类型指的是：输入、buffer、输出
  */
class MyAverge2 extends Aggregator[Employee, Averge, Double]{
  /**
    * 指定初始值
    * @return
    */
  override def zero: Averge = Averge(0.0, 0.0)

  /**
    * 局部聚合：每一个分区的聚合方法
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: Averge, a: Employee): Averge = {
    b.sum += a.salary
    b.count += 1
    b
  }

  /**
    * 全局聚合方法：将每一个分区（buffer）进行总的聚合
    * @param b1
    * @param b2
    * @return
    */
  override def merge(b1: Averge, b2: Averge): Averge = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  /**
    * 最终结果输出的方法，可以在该方法中进行最后的操作
    * @param reduction
    * @return
    */
  override def finish(reduction: Averge): Double = reduction.sum / reduction.count

  /**
    * 设置中间结果的编码器
    * @return
    */
  override def bufferEncoder: Encoder[Averge] = Encoders.product

  /**
    * 输出结果的编码器
    * @return
    */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class Employee(salary: Double)
case class Averge(var sum: Double, var count: Double)
