package com.qf.gp1922.day12

import org.apache.spark.sql.SparkSession

/**
  * 实现聚合函数
  */
object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    // 注册UDAF函数
    spark.udf.register("myaverage", new MyAverAge)

    val df = spark.read.json("dir/employee.json")

    df.createOrReplaceTempView("t_employee")

    val res = spark.sql("select myaverage(age) from t_employee")

    res.show()

    spark.stop()
  }
}
