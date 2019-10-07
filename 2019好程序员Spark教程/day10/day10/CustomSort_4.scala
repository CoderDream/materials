package com.qf.gp1922.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第四种方式：可以用Ordering自带的on方法
  */
object CustomSort_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val userInfo = sc.parallelize(Array("mimi 28 85", "bingbing 30 85", "yuanyuan 18 90"))

    val personRDD: RDD[(String, Int, Int)] = userInfo.map(x => {
      val arr = x.split(" ")
      val name = arr(0)
      val age = arr(1).toInt
      val fv = arr(2).toInt

      (name, age, fv)
    })

    /**
      * 下面调用过程中：
      * (Int, Int)：指定函数返回的类型
      * (String, Int, Int)：on方法输入参数的类型
      */
    // 指定隐式的值
    implicit val ord = Ordering[(Int, Int)].on[(String, Int, Int)](x => (-x._3, x._2))
    // 排序
    val sorted: RDD[(String, Int, Int)] = personRDD.sortBy(x => x)

    println(sorted.collect.toBuffer)


    sc.stop()
  }
}