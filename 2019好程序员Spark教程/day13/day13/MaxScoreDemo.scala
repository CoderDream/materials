package com.qf.gp1922.day13

import org.apache.spark.sql.SparkSession

/**
  * 用开窗函数实现需求
  * 需求：每个班级最高成绩的学生信息
  */
object MaxScoreDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("dir/Score.json")

    df.createOrReplaceTempView("t_score")

//    val res = spark.sql("select class, max(score) maxscore from t_score group by name")
    val res = spark.sql("select * from " +
      "(select name, class, score, row_number() over(partition by class order by score desc) rank " +
      "from t_score) t where t.rank=1")

    res.show

    spark.stop()
  }
}
