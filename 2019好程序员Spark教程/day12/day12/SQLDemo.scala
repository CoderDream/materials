package com.qf.gp1922.day12

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 用SQL语言风格实现流程
  */
object SQLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    // 获取数据生成DataFrame
    val df = spark.read.json("dir/people.json")

    // 注册一张临时表, 使用最多. 该表的作用域在当前的Session中
    df.createOrReplaceTempView("t_people")

    // 注册一张全局的临时表,在该应用程序中都可以访问，
    // 访问的时候需要全路径访问
    df.createGlobalTempView("t_global_people")

    // 开始用sql进行操作
    val res = spark.sql("select * from t_people")
    // 访问global表
    spark.sql("select * from global_temp.t_global_people").show()

    // 输出， save方法默认存储的格式是parquet
//    res.show()
//    res.write.mode(SaveMode.Append).save("hdfs://node01:9000/out-20190806-1")
//    res.write.mode(SaveMode.Append).csv("hdfs://node01:9000/out-20190806-2")

    spark.stop()
  }
}
