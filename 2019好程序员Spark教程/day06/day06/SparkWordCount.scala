package com.qf.gp1922.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境
      */
    // 创建配置文件对象
    val conf: SparkConf = new SparkConf()
    // 配置应用程序名称
    conf.setAppName("sparkwordcount")
    // 配置运行模式，IDEA下必须为local模式
    // local是调用一个线程来运行该job
    // local[2]是调用两个线程来运行该job
    // local[*]是调用当前所有空闲的线程来运行该job
    conf.setMaster("local[2]")
    // Spark的上下文对象，也称为集群的入口类
    val sc: SparkContext = new SparkContext(conf)

    // 获取数据
    val lines: RDD[String] = sc.textFile("hdfs://node01:9000/files")
//    val lines: RDD[String] = sc.textFile(args(0))

    // 对数据做切分，生成一个个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 将单词生成一个元组->(word, 1), 便于计数统计
    val tup: RDD[(String, Int)] = words.map((_, 1))

    // 开始聚合统计
    val sumed: RDD[(String, Int)] = tup.reduceByKey(_+_)

    // 降序排序
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2, false)

    // 结果输出
//    sorted.collect.foreach(println) // foreach是Scala的方法
//    sorted.foreach(println) // foreach是Spark提供的api
    println(sorted.collect.toBuffer)

    // 存储到HDFS
//    sorted.saveAsTextFile("hdfs://node01:9000/out-20190729-1")
//    sorted.saveAsTextFile(args(1))

    // 释放sc资源
    sc.stop()
  }
}
