package com.qf.gp1922.day12

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.uncommons.maths.statistics.DataSet

/**
  * 通过反射的方式将RDD生成DataSet
  */
object RDD2DataSet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 获取数据并切分
    val peopleRDD = sc.textFile("dir/people.txt").map(_.split(", "))
    // 反射生成元组
    val tupRDD: RDD[People] = peopleRDD.map(x => People(x(0), x(1).toInt))

    import spark.implicits._ // SQLContext

    // 调用toDS生成DataSet
    val ds: Dataset[People] = tupRDD.toDS() // 反射生成Schema信息

    val rdd: RDD[People] = ds.rdd
    val df: DataFrame = ds.toDF()

    ds.show()

    sc.stop()
    spark.stop()
  }
}
