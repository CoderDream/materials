package com.qf.gp1922.day08.serializedemo

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 任务执行时，处理一条数据创建一次对象, 效率及其低下，不建议在算子内创建对象
  */
object SerializeTest_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(Array("xiaoli", "xiaofang", "xiaolin"))

    // map方法中的函数是在Executor的某个Task中执行的
    val res = lines.map(x => {
      // 该对象是在Executor端创建的
      val rules = new Rules
      // 获取task的hostname，也就是判断该task是在哪个节点执行的
      val hostname = InetAddress.getLocalHost.getHostName
      // 获取当前线程名称
      val threadName = Thread.currentThread().getName
      //rules是在Executor中使用的
      (hostname, threadName, rules.rulesMap.getOrElse(x, 0), rules.toString)
    })

    println(res.collect.toBuffer)

    sc.stop()
  }
}
