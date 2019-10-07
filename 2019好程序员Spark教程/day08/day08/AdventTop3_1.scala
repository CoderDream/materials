package com.qf.gp1922.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据字段：时间戳 省份 城市 userId adId
  * 需求：求每个省份的点击广告数的top3
  */
object AdventTop3_1 {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf = new SparkConf().setAppName("adventtop3").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据并切分
    val logs = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/advert/Advert.log")
    val logArr: RDD[Array[String]] = logs.map(_.split("\t"))

    // 提取要分析需求需要的数据
    val provinceAndAdId: RDD[(String, Int)] = logArr.map(x => (x(1) + "_" + x(4), 1))

    // 将每个省份对应的多个广告进行点击量的统计
    val aggrProvinceAndAdId: RDD[(String, Int)] = provinceAndAdId.reduceByKey(_+_)

    // 为了方便接下来进行省份的分组并组内排序，需要将数据进行整合
    val provinceAndAdIdTup: RDD[(String, String, Int)] = aggrProvinceAndAdId.map(tup => {
      val splited = tup._1.split("_")
      val province = splited(0)
      val adId = splited(1)
      (province, adId, tup._2)
    })

    // 按照省份进行分组
    val groupedPro: RDD[(String, Iterable[(String, String, Int)])] =
      provinceAndAdIdTup.groupBy(_._1)

    // 组内排序
    val res: RDD[(String, List[(String, String, Int)])] =
      groupedPro.mapValues(x => x.toList.sortWith(_._3 > _._3).take(3))

    println(res.collect.toBuffer)

    sc.stop()
  }
}
