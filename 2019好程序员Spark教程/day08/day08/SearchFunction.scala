package com.qf.gp1922.day08

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class SearchFunctions(val query: String) {
  //第一个方法是判断输入的字符串是否存在query 存在返回true,不存在返回false
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    // 问题:"isMatch"表示"this.isMatch"，因此我们要传递整个"this"
    rdd.filter(x => this.isMatch(x)) // 等价于：rdd.filter(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[String] = {
    // 问题:"query"表示"this.query"，因此我们要传递整个"this"
    rdd.filter(x => x.contains(this.query))
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[String] = {
    // 安全:只把我们需要的字段拿出来放入局部变量中
    val _query = this.query
    rdd.filter(x => x.contains(_query))
  }
}

object SearchFunctions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SearchFunctions")
    conf.setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(List("hello java", "hello scala hello", "hello hello"))

    val sf = new SearchFunctions("hello")
//        sf.getMatchesFunctionReference(rdd)
//        sf.getMatchesFieldReference(rdd)
//        sf.getMatchesNoReference(rdd)
  }
}