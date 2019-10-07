package com.qf.gp1922.day13

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 用jdbc的方式写数据
  */
object WriteData2Jdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    val employee = spark.read.json("dir/employee.json")
    val nameAndSalaryDF: DataFrame = employee.select("name", "salary")

    // 方式一
    //    val prop = new Properties()
    //    prop.put("user", "root")
    //    prop.put("password", "root")
    //    val url = "jdbc:mysql://node03:3306/bigdata"
    //    nameAndSalaryDF.write.mode(SaveMode.Append).jdbc(url, "employees", prop)

    // 方式二
    //    nameAndSalaryDF.write.mode(SaveMode.Append).format("jdbc")
    //        .option("user", "root")
    //        .option("password", "root")
    //        .option("dbtable", "employees")
    //        .option("url", "jdbc:mysql://node03:3306/bigdata")
    //        .save()

    // 方式三
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    nameAndSalaryDF.write
      .option("createTableColumnTypes", "name varchar(200),salary int")
      .jdbc("jdbc:mysql://node03:3306/bigdata", "employees", prop)

    spark.stop()
  }
}
