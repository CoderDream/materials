package com.qf.gp1922.day12

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 生成并操作DataFrame
  * 将RDD转换为DataFrame
  */
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据
    val peopleRDD = sc.textFile("dir/people.txt")
    // 切分
    val linesRDD: RDD[Array[String]] = peopleRDD.map(_.split(", "))
    // 将数据生成tuple，便于DataFrame的转换
    val tupRDD: RDD[(String, Int)] = linesRDD.map(x => (x(0), x(1).toInt))

    // SparkSession上下文
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 引用SqlContext上下文
    import spark.implicits._
    // toDF方法属于SQLContext上下文的方法，在RDD转换DataFrame的时候，需要我们指定列名
    val df: DataFrame = tupRDD.toDF("name", "age")

    df.show()

    sc.stop()
    spark.stop()
  }
}
