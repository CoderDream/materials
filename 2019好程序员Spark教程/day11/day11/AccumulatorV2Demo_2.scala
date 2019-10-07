package com.qf.gp1922.day11

import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义Accumulator
  */
object AccumulatorV2Demo_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val nums = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 2)
    // 1、获取自定义Accumulator累计器的实例
    val accmulator = new MyAccumulator()

    // 2、注册
    sc.register(accmulator, "acc")

    // 3、进行累加
    nums.foreach(x => accmulator.add(x))

    println(accmulator.value)

    sc.stop()
  }
}

/**
  * AccumulatorV2[IN, OUT]: 需要定义输入类型和输出类型
  */
class MyAccumulator extends AccumulatorV2[Int, Int] {
  // 初始化一个输出值的变量
  private var sum: Int = _

  /**
    * 检查值是否为空
    * @return
    */
  override def isZero: Boolean = sum == 0

  /**
    * copy一个新的累加器，用于迭代处理的
    * @return
    */
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyAccumulator
    acc.sum = this.sum
    acc
  }

  /**
    * 重置一个累加器，相当于将累加器的数据清零
    */
  override def reset(): Unit = sum = 0

  /**
    * 局部聚合：每个分区中进行累加的过程
    * @param v
    */
  override def add(v: Int): Unit = {
    sum += v
  }

  /**
    * 全局聚合：将各个分区的结果进行合并的过程
    * @param other
    */
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }

  /**
    * 最终的结果，可以在该方法中对结果数据进一步的操作并返回
    * @return
    */
  override def value: Int = sum
}
