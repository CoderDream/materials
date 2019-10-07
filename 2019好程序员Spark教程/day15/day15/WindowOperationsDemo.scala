package com.qf.gp1922.day15

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 窗口操作的使用
  * 需求：批次间隔为2秒，但每次展示的结果范围为10秒，10秒展示一次
  */
object WindowOperationsDemo {
  def main(args: Array[String]): Unit = {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 实例化Streaming的上下文
    val ssc = new StreamingContext(sc, Durations.seconds(2))

    // 从NetCat服务里获取数据
    val logs: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 6666)
    // 分析
    val tups = logs.flatMap(_.split(" ")).map((_, 1))
    val res: DStream[(String, Int)] =
      tups.reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(10), Seconds(10))

    res.print()

    ssc.start() // 提交作业到集群
    ssc.awaitTermination() // 线程等待，等待处理任务
  }
}
