package com.qf.gp1922.day12

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求：统计每个部门中的所有大于20岁的不同性别的员工的平均薪资和平均年龄
  * 思路：
  *   1、统计年龄在20岁以上的员工
  *   2、需要根据部门和性别进行分组
  *   3、统计平均薪资和平均年龄
  */
object DepAvgSalaryAndAgeDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._ // 拿到SqlContext上下文
    import org.apache.spark.sql.functions._  // 拿到用于sql处理的常用方法

    // 获取数据
    val employee = spark.read.json("dir/employee.json")
    val department = spark.read.json("dir/department.json")

    // 统计
    val res: DataFrame = employee
      .filter("age > 20") // 统计年龄在20岁以上的员工
      .join(department, $"depId" === $"id")
      .groupBy(department("id"), employee("gender")) // 需要根据部门和性别进行分组
      .agg(avg(employee("salary")), avg(employee("age")))

    res.show()

    spark.stop()
  }
}
