package com.qf.gp1922.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SubjectAccessCache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 学科信息
    val subjects = Array("http://bigdata.learn.com", "http://h5.learn.com", "http://ui.learn.com", "http://java.learn.com", "http://android.learn.com")

    // 获取数据并切分生成元组
    val tups = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/subjectaccess/access.txt")
      .map(line => (line.split("\t")(1), 1))

    // 开始聚合，得到各个模块的访问量
    val aggred: RDD[(String, Int)] = tups.reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK) //.cache()

    // 按照学科信息进行分组并组内排序取top3
    for (subject <- subjects) {
      val filtered: RDD[(String, Int)] = aggred.filter(_._1.startsWith(subject))
      // 按照学科进行过滤，得到该学科对应的所有的模块的访问量
      val res: Array[(String, Int)] = filtered.sortBy(_._2, false).take(3)
      println(res.toBuffer)
    }

    sc.stop()
  }
}
