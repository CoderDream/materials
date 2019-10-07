package com.qf.gp1922.day12

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
  * 实现UDF函数
  */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("dir/people.json")

    // 注册UDF函数, 两种实现方式
//    spark.udf.register("concatname", new ConcatNameUDF, StringType)
    spark.udf.register("concatname", (str: String) => "name:" + str)

    // 注册临时表
    df.createOrReplaceTempView("t_people")

    // 调用UDF函数进行查询
    spark.sql("select concatname(name), age from t_people").show()

    spark.stop()
  }
}
