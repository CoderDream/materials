package com.qf.gp1922.day10

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 自定义分区器实现自己的分区逻辑
  */
object CustomPartitioner_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据并切分生成元组
    val tups = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/subjectaccess")
      .map(line => (line.split("\t")(1), 1))

    // 聚合生成各个模块的访问量
    val aggred: RDD[(String, Int)] = tups.reduceByKey(_+_).cache()

    // 获取学科信息并返回学科信息及url和访问量
    val subjectAndUrlAndCount: RDD[(String, (String, Int))] = aggred.map(tup => {
      val url = tup._1
      val count = tup._2
      val subject = new URL(url).getHost
      (subject, (url, count))
    }).cache()

    // 调用默认的分区器查看分区的情况, 出现了多个学科信息放到了同一个分区文件中，出现了哈希碰撞
//    val partitioned: RDD[(String, (String, Int))] = subjectAndUrlAndCount.partitionBy(new HashPartitioner(5))
//    partitioned.saveAsTextFile("d://out-20190802-3")

    // 获取到所有的学科信息
//    val subjects: RDD[String] = subjectAndUrlAndCount.map(_._1)
    val subjects: RDD[String] = subjectAndUrlAndCount.keys.distinct
    val subjectsArr = subjects.collect

    // 调用自定义分区器进行分区
    val partitioned = subjectAndUrlAndCount.partitionBy(new SubjectPartitoner(subjectsArr))

    // 分组排序并取top3
    val res: RDD[(String, (String, Int))] = partitioned.mapPartitions(it => {
      it.toList.sortWith(_._2._2 > _._2._2).take(3).iterator
    })

    res.saveAsTextFile("d://out-20190802-4")

    sc.stop()
  }
}

/**
  * 自定义分区器
  */
class SubjectPartitoner(subjects: Array[String]) extends Partitioner{
  // 用于存储学科信息和对应的分区号
  val subjectAndNum = new mutable.HashMap[String, Int]()

  // 计数器
  var i = 0
  for (subject <- subjects) {
    subjectAndNum += (subject -> i)
    i += 1
  }

  // 获取分区数
  override def numPartitions: Int = subjects.length
  // 获取分区号
  override def getPartition(key: Any): Int = subjectAndNum.getOrElse(key.toString, 0)
}

