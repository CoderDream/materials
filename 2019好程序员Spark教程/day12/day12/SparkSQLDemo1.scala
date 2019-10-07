package com.qf.gp1922.day12

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * SparkSQL的初始化
  */
object SparkSQLDemo1 {
  def main(args: Array[String]): Unit = {
    // 第一种创建方式
    val spark1: SparkSession = SparkSession.builder()
      .appName("sparksqldemo")
      .master("local[2]")
      .getOrCreate

    // 第二种创建方式
    val conf = new SparkConf().setAppName("sparksqldemo").setMaster("local")
    val spark2 = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // 第三种创建方式
    val spark3 = SparkSession.builder()
      .appName("sparksqldemo")
      .master("local[2]")
      .enableHiveSupport() // 启用hive支持
      .getOrCreate()


    spark1.stop()
    spark2.stop()
    spark3.stop()
  }
}
