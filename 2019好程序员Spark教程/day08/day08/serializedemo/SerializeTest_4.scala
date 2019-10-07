package com.qf.gp1922.day08.serializedemo

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 不用在Driver端去创建对象，Rules不用实现序列化
  */
object SerializeTest_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(Array("xiaoli", "xiaofang", "xiaolin"))

    // map方法中的函数是在Executor的某个Task中执行的
    val res = lines.map(word => {
      // 获取task的hostname，也就是判断该task是在哪个节点执行的
      val hostname = InetAddress.getLocalHost.getHostName
      // 获取当前线程名称
      val threadName = Thread.currentThread().getName
      // rules是在Executor中使用的
      (hostname, threadName, ObjectRules.rulesMap.getOrElse(word, 0), ObjectRules.toString)
    })

    println(res.collect.toBuffer)
//    res.saveAsTextFile("hdfs://node01:9000/out-20181128-1")

    sc.stop()
  }
}
