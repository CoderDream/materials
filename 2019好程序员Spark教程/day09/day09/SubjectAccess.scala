package com.qf.gp1922.day09

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubjectAccess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据
    val logs = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/subjectaccess/access.txt")

    // 切分并生成元组，便于聚合
    val tups: RDD[(String, Int)] = logs.map(line => {
      val url = line.split("\t")(1)
      (url, 1)
    })

    // 将相同的url进行聚合，求出各个模块的访问量
    val aggred: RDD[(String, Int)] = tups.reduceByKey(_+_)

    // 接下来需要拿到每个学科，然后按照学科进行分组
    val subjectAndUrlAndCount: RDD[(String, (String, Int))] = aggred.map(tup => {
      val url = tup._1 // 用户请求的url
      val count = tup._2 // 模块的访问量
      val subject = new URL(url).getHost // 学科信息
      (subject, (url, count))
    })

    // 按照学科进行分组
    val grouped: RDD[(String, Iterable[(String, Int)])] = subjectAndUrlAndCount.groupByKey

    // 组内排序并取top3
    val res: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortWith(_._2 > _._2).take(3))

    println(res.collect.toBuffer)

    sc.stop()
  }
}
