package com.qf.gp1922.day12

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 通过StructType生成Schema
  */
object DataFrameDemo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 获取数据并切分生成Row类型的数据
    val peopleRowRDD: RDD[Row] = sc.textFile("dir/people.txt").map(x => {
      val fields = x.split(", ")
      Row(fields(0), fields(1).toInt)
    })

    // 通过StructType生成Schema
    val schema: StructType = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    // 转换生成DataFrame
    val df: DataFrame = spark.createDataFrame(peopleRowRDD, schema)

    df.show

    sc.stop()
    spark.stop()
  }
}
