package com.qf.gameanalyze

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object GameKPI {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GameKPI")
      .setMaster("local[2]")
      .set("es.nodes", "node01,node02,node03")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

//    val queryTime = args(0)
    val queryTime = "2016-02-02 00:00:00"
    val startTime = TimeUtils(queryTime)
    val endTime = TimeUtils.updateCalendar(+1)
    val startTimeMorrow = TimeUtils.updateCalendar(+1)
    val endTimeMorrow = TimeUtils.updateCalendar(+2)


    // 查询es数据的条件
    val query =
      s"""
         {"query":{"match_all":{}}}
       """.stripMargin

    // 获取es数据, 返回的类型中String是对应“_id”字段值，代表id对应的那行数据
    val queryRDD: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("gamelogs", query)

    // 将id过滤掉, Map[String, AnyRef]: String是字段名称，AnyRef是字段值
    val filteredRDD: RDD[collection.Map[String, AnyRef]] = queryRDD.map(_._2).filter(line => {
      String.valueOf(line.getOrElse("current_time", -1)).substring(0, 1).equals("2")
    })

    // 切分数据
    val splitedRDD: RDD[Array[String]] = filteredRDD.map(line => {
      val et = String.valueOf(line.getOrElse("event_type", "-1")) // 事件类型
      val time = String.valueOf(line.getOrElse("current_time", "-1"))
      val user = String.valueOf(line.getOrElse("user", ""))

      Array(et, time, user)
    })


    // 很多指标的统计都需要发过滤，可以将过滤的逻辑封装为工具类，减少代码冗余
    // SimpleDateFormat是线程不安全的，可以加锁，也可以用另外一个线程安全的对象：FastDateFormat
    // 最好不要在算子内部去new一个对象，会占用很多内存资源
    //    splitedRDD.filter(x => {
    //      val time = x(1)
    //      val sdf = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    //      val time_long = sdf.parse(time).getTime
    //    })

    // 日新增：DNU
    val dnu = splitedRDD.filter(fields => {
      FilterUtils.filterByTimeAndType(fields, EventType.REGISTER, startTime, endTime)
    })

    // 日活跃：DAU
    val filterByTimeAndTypes = splitedRDD.filter(fields => {
      FilterUtils.filterByTime(fields, startTime, endTime) &&
        FilterUtils.filterByTypes(fields, EventType.REGISTER, EventType.LOGIN)
    })
    // 在这一天当中，可能有用户登录多次的情况，需要去重
    val dau = filterByTimeAndTypes.map(_(2)).distinct

    // 次日留存
    // 第一天的新增用户拿出来，需要和第二天的登录的用户进行join
    val dnuTuple = dnu.map(fields => (fields(2), 1))
    // 第二天登录的用户
    val day2Login = splitedRDD.filter(fields => {
      FilterUtils.filterByTimeAndType(fields, EventType.LOGIN, startTimeMorrow, endTimeMorrow)
    })
    // 第二天登录的用户需要去重，并且生成tuple
    val day2Uname = day2Login.map(_(2)).distinct.map((_, 1))
    // 进行join
    val morrowKeep = dnuTuple.join(day2Uname)
//    val morrowKeep2 = dnuTuple.intersection(day2Uname)


    println("日新增用户：" + dnu.map(_ (2)).collect.toBuffer)
//    println("日新增用户数：" + dnu.count())
//    println("日活跃用户数：" + dau.count())
    println("次日留存数：" + morrowKeep.count())
//    println("次日留存数：" + morrowKeep2.count())
    // 次日留存率：次日留存数/前一天的新增用户数   morrowKeep2.count()/dnu

    sc.stop()
  }
}
