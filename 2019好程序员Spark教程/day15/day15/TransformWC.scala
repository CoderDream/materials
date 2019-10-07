package com.qf.gp1922.day15

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWC {
  def main(args: Array[String]): Unit = {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 实例化Streaming的上下文
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    // 从NetCat服务里获取数据
    val logs: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 6666)
    // 分析
    val res: DStream[(String, Int)] = logs.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })

    res.print()

    ssc.start() // 提交作业到集群
    ssc.awaitTermination() // 线程等待，等待处理任务
  }
}
