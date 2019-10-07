package com.qf.gp1922.day11

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

//    var sum: Int = 0

    val numsRDD = sc.parallelize(List(1,2,3,4,5,6))
// 用foreach或map并不能实现将各Executor计算的结果返回给Driver端的某个共享变量
//    numsRDD.foreach(x => sum += x)
//    numsRDD.map(x => sum += x)
//    sum = numsRDD.reduce(_+_)

    // 使用Accumulator实现给共享变量的聚合值的过程
    val sum: Accumulator[Int] = sc.accumulator(0)
    numsRDD.foreach(x => sum += x)


    println(sum)

    sc.stop()
  }
}
