package com.qf.gp1922.day13

import org.apache.spark.sql.{SparkSession, TypedColumn}

object UDAFDemo_DataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json("dir/employee.json")
    val salaryDF = df.select("salary")
    val ds = salaryDF.as[Employee]

    // 通过实例化的方式拿到自定义函数
    val avergeSalary: TypedColumn[Employee, Double] =
      new MyAverge2().toColumn.name("avergesalary")

    val res = ds.select(avergeSalary)

    res.show()
  }
}
