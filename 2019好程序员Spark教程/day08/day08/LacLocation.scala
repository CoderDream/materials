package com.qf.gp1922.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LacLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("location").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取用户访问数据
    val logs = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/lacduration/log")

    // 切分用户数据
    val userInfo: RDD[((String, String), Long)] = logs.map(line => {
      val fields = line.split(",")
      val phone = fields(0)
      val time = fields(1).toLong
      val lac = fields(2)
      val eventType = fields(3)
      // 判断时间是否是建立会话时间，如果是，前面加“-”，如果不是，原样返回，这样方便以后统计时长
      val time_long = if (eventType.equals("1")) -time else time

      ((phone, lac), time_long)
    })

    // 统计每个用户在所有基站停留的总时长
    val aggred: RDD[((String, String), Long)] = userInfo.reduceByKey(_+_)

    // 整合数据，方便将基站信息join过来
    val lacAndPhoneAndTime: RDD[(String, (String, Long))] = aggred.map(tup => {
      val phone = tup._1._1
      val lac = tup._1._2
      val time = tup._2
      (lac, (phone, time))
    })

    // 获取基站信息
    val lacInfo = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/lacduration/lac_info.txt")
      .map(line => {
        val fields = line.split(",")
        val lac = fields(0)
        val lang = fields(1)
        val lat = fields(2)
        (lac, (lang, lac))
      })

    // 将聚合后的用户信息和基站信息进行join
    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAndPhoneAndTime join lacInfo

    // 整合数据，方便按照手机号进行分组并排序
    val phoneAndTimeAndLangLat: RDD[(String, (Long, (String, String)))] = joined.map(tup => {
      val phone = tup._2._1._1
      val time = tup._2._1._2
      val langAndLat = tup._2._2

      (phone, (time, langAndLat))
    })

    // 按照手机号进行分组
    val grouped: RDD[(String, Iterable[(Long, (String, String))])] =
      phoneAndTimeAndLangLat.groupByKey()

    // 组内排序
    val res: RDD[(String, List[(Long, (String, String))])] =
      grouped.mapValues(_.toList.sortWith(_._1 > _._1).take(2))

    res.foreach(println)

    sc.stop()
  }
}
