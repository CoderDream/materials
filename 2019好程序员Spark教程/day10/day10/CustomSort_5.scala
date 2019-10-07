package com.qf.gp1922.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第五种方式：可以用元组封装排序条件的方式实现自定义排序
  */
object CustomSort_5 {
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

    // 排序
    val sorted: RDD[(String, Int, Int)] = personRDD.sortBy(x => (-x._3, x._2))

    println(sorted.collect.toBuffer)


    sc.stop()
  }
}