package com.qf.gp1922.day13

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * sparksql的输入输出
  */
object InputOutputDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

//    spark.read.json("dir/people.json")
//    spark.read.text("dir/people.json").show()
//    spark.read.textFile("dir/people.json").show()
    val loaddata = spark.read.format("text").load("dir/people.txt")


//    loaddata.write.format("json").save("d://out")
//    loaddata.write.save("d://out-1")
    loaddata.write.mode(SaveMode.Append).text("d://out-2")

    spark.stop()
  }
}
