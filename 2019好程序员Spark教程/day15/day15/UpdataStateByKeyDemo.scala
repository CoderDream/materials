package com.qf.gp1922.day15

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * updataStateByKey会将历史结果拿到当前批次进行进一步的计算
  * 注意：updataStateByKey没有存储历史结果的功能，
  * 所以updataStateByKey只能去checkpoint的目录中去拿历史结果
  */
object UpdataStateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UpdataStateByKeyDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5)) // Durations.seconds(5) == Seconds(5)

    // checkpoint  hdfs
    ssc.checkpoint("d://cp-20190810-1")

    // 获取数据
    val logs = ssc.socketTextStream("node01", 6666)
    // 开始统计
    val tups: DStream[(String, Int)] = logs.flatMap(_.split(" ")).map((_, 1))

    /**
      * updateStateByKey需要的参数：
      * updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      *   函数的参数中：
      *     K是指进行聚合的时候的那个相同的key，现在是指的单词
      *     Seq[V]是指当前批次相同key对应的value，会将相同key对应的value统计的放到Seq里：Seq(1,1,1,1,1,...)
      *     Option[S]是指历史结果相同key对应的结果数据，可能有值，也可能没有值，所以用Option来封装：如果有值Some(3),如果没有值则为None
      * partitioner: Partitioner,
      *   是指指定分区器
      * rememberPartitioner: Boolean
      *   是指是否记录父RDD的分区信息
      */
    val res: DStream[(String, Int)] = tups.updateStateByKey(
      func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    res.print

    ssc.start()
    ssc.awaitTermination()
  }

  // (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(tup => {
      (tup._1, tup._2.sum + tup._3.getOrElse(0))
    })
  }
}
