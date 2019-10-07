package com.qf.gp1922.day10

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomPartitioner_1 {
}

class CustomerPartitoner(numPartiton:Int) extends Partitioner{
  // 返回分区的总数
  override def numPartitions: Int = numPartiton
  // 根据传入的Key返回分区的索引
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numPartiton
  }
}
object CustomerPartitoner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CustomerPartitoner").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //zipWithIndex该函数将RDD中的元素和这个元素在RDD中的ID（索引号）组合成键/值对。
    val rdd = sc.parallelize(0 to 10, 2).zipWithIndex()
    println(rdd.collect.toBuffer)

    val func = (index: Int, iter: Iterator[(Int, Long)]) => {
      iter.map(x => "[partID:" + index + ", value:" + x + "]")
    }

    val r = rdd.mapPartitionsWithIndex(func).collect()
    r.foreach(println)

    val rdd2 = rdd.partitionBy(new CustomerPartitoner(5))
    val r1 = rdd2.mapPartitionsWithIndex(func).collect()
    println("----------------------------------------")
    r1.foreach(println)
    println("----------------------------------------")

    sc.stop()
  }
}