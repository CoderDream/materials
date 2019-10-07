package com.qf.gp1922.day11

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：按照用户访问的IP地址找到对应的省份并求所属区域的用户访问量
  * 思路：
  * 1、获取ip基础数据，并广播出去
  * 2、获取用户访问信息
  * 3、通过用户的ip地址找到对应的省份（二分查找）
  * 4、通过找到的省份来统计区域访问量
  * 5、将结果输出到mysql
  */
object IPSearchDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取ip基础数据并切分
    val ipInfo = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/ipsearch/ip.txt")
      .map(line => {
        val fields = line.split("\\|")
        val startIP = fields(2) // 其实ip
        val endIP = fields(3) // 结束ip
        val province = fields(6) // ip段对应的省份
        (startIP, endIP, province)
      })

    // 将ip段的基础数据进行广播
    val broadcastIPInfo: Broadcast[Array[(String, String, String)]] =
      sc.broadcast(ipInfo.collect)

    // 获取用户点击流日志并切分, 根据二分查找到用户ip对应的省份，返回(省份， 1)， 便于后期聚合
    val logs = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/ipsearch/http.log")
      .map(line => {
        val userIP = line.split("\\|")(1) // 用户的ip
        val userIP_Long = ip2Long(userIP) // long类型的用户ip
        // 广播过来的ip基础数据
        val ipInfoArr: Array[(String, String, String)] = broadcastIPInfo.value
        // 通过二分查找，找到用户ip对应的ip段的下标
        val index = binarySearch(ipInfoArr, userIP_Long)
        val province = ipInfoArr(index)._3

        (province, 1)
      })

    // 统计省份对应的访问量
    val aggred = logs.reduceByKey(_ + _)

    println(aggred.collect.toBuffer)

    // 将结果存到mysql
    aggred.foreachPartition(data2MySQL)


    sc.stop()
  }

  /**
    * 将ip转换为long类型
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分查找用户ip对应的ip基础数据的下标
    *
    * @param arr
    * @param ip
    */
  def binarySearch(arr: Array[(String, String, String)], ip: Long): Int = {
    // 定义要查找的开始值和结束值
    var start = 0
    var end = arr.length - 1

    while (start <= end) {
      // 中间值
      val middle = (start + end) / 2
      // 判断
      if ((ip >= arr(middle)._1.toLong) && (ip <= arr(middle)._2.toLong)) {
        return middle
      } else if (ip < arr(middle)._1.toLong) {
        end = middle - 1
      } else {
        start = middle + 1
      }

    }

    -1
  }

  // 写入数据到mysql的函数
  val data2MySQL = (it: Iterator[(String, Int)]) => {
    var conn: Connection = null;
    var ps: PreparedStatement = null;

    val sql = "insert into location_info(location, counts, access_date) values(?,?,?)"

    val jdbcUrl = "jdbc:mysql://node03:3306/bigdata?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "root"

    try {
      conn = DriverManager.getConnection(jdbcUrl, user, password)
      it.foreach(tup => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, tup._1)
        ps.setInt(2, tup._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

}
