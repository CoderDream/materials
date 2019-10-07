package com.qf.gp1922.day09

import org.apache.spark.{SparkConf, SparkContext}

/**
  * checkpoint的步骤：
  *   1、设置checkpoint的存储目录
  *   2、cache
  *   3、开始checkpoint
  */
object CheckpointDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDemo").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("hdfs://node01:9000/cp-20190801-1")

    val rdd =  sc.textFile("hdfs://node01:9000/files").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).cache()
    // checkpoint
    rdd.checkpoint()
    // 判断是否checkpoint
    val iscp = rdd.isCheckpointed // 打印的是false，因为此时还没有调用action


    rdd.saveAsTextFile("hdfs://node01:9000/out-20190801-3")

    val iscp1 = rdd.isCheckpointed

    // 查看checkpoint的目录
    val cpfile = rdd.getCheckpointFile

    println("action之前：" + iscp)
    println("action之后：" + iscp1)
    println(cpfile) //查看存储的位置


    sc.stop()
  }
}
