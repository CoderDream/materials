package com.qf.gp1922.day12

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * DSL语言风格操作
  */
object DSLDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 获取数据直接生成DataFrame
    val df = spark.read.json("dir/people.json")

    // DSL语言风格操作
    df.show()
    df.select("name").show()
    // 打印Schema信息
    df.printSchema()

    spark.stop()
  }
}
