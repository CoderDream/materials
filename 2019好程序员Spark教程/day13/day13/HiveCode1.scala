package com.qf.gp1922.day13

import org.apache.spark.sql.{Row, SparkSession}

object HiveCode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveCode")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse") // 配置元数据库的目录
      .master("local[*]")
      .enableHiveSupport() // 启用Hive支持
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src_3 (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH  'dir/kv1.txt' INTO TABLE src_3")
    sql("SELECT * FROM src_3").show()
    sql("SELECT COUNT(*) FROM src_3").show()
    sql("SELECT key, value FROM src_3 WHERE key < 10 ORDER BY key").show()

    spark.stop()
  }
}

case class Record(key: Int, value: String)