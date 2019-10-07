package com.qf.gp1922.day11

import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 简单的累加器的使用
  * Spark提供了用于简单聚合的Accumulator累加器，比如LongAccumulator或DoubleAccumulator
  */
object AccumulatorV2Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val nums1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 2)
    val nums2 = sc.parallelize(List(1.1,2.2,3.3,4.4), 2)

//    // 首先需要进行注册，注册并初始化一个累加器
//    def longAcc(name: String): LongAccumulator = {
//      val acc = new LongAccumulator
//      sc.register(acc, name)
//      acc
//    }
//
//    // 累加器的返回值
//    val acc1: LongAccumulator = longAcc("LongAccumulator")
//    nums1.foreach(x => acc1.add(x))
//    println(acc1.value)

    // 首先需要进行注册，注册并初始化一个累加器
    def doubleAcc(name: String): DoubleAccumulator = {
      val acc = new DoubleAccumulator
      sc.register(acc, name)
      acc
    }

    // 累加器的返回值
    val acc1: DoubleAccumulator = doubleAcc("LongAccumulator")
    nums2.foreach(x => acc1.add(x))
    println(acc1.value)


    sc.stop()
  }
}
