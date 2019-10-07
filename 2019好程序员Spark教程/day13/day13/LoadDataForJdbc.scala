package com.qf.gp1922.day13

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 对接jdbc的获取方式
 */
object LoadDataForJdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

//     获取方式一
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    val url = "jdbc:mysql://node03:3306/bigdata"
//
//    val df: DataFrame = spark.read.jdbc(url, "person", prop)

    // 获取方式二
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://node03:3306/bigdata")
      .option("dbtable", "person")
      .option("user", "root")
      .option("password", "root")
      .load()


    df.show

    spark.stop
  }
}
