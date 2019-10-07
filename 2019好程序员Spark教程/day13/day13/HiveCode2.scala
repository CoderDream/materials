package com.qf.gp1922.day13

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 访问Hive元数据库
  */
object HiveCode2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
//      .master("local[2]")
//      .config("spark.warehouse.dir", "hdfs://node01:9000/sparksql_warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("create table if not exists src_6(key int, value string)")
    spark.sql("load data local inpath '/root/kv1.txt' into table src_6")

    val df = spark.sql("select * from src_6 where key <= 10 order by key")

    df.write.mode(SaveMode.Append).csv("hdfs://node01:9000/out-20190807-2")

    spark.stop()
  }
}
