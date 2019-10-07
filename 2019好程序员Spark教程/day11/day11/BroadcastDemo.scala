package com.qf.gp1922.day11

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastTest").setMaster("local")
    val sc = new SparkContext(conf)
    //list是在driver端创建也相当于是本地变量
    val str = "hello java"
    //封装广播变量并进行广播，（该方法也是transformation，只有在action的时候才执行）
    val broadcast = sc.broadcast(str)
    //算子部分是在Excecutor端执行
    val lines = sc.textFile("c://data/file.txt")
    //使用广播变量进行数据处理 value可以获取广播变量的值
    val filterStr = lines.filter(x => x.contains(broadcast.value))

    filterStr.foreach(println)

    sc.stop
  }
}
