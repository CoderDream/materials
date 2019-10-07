package com.qf.gp1922.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第一种方式：可以用普通类或样例类来实现自定义排序
  */
object CustomSort_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val userInfo = sc.parallelize(Array("mimi 28 85", "bingbing 30 85", "yuanyuan 18 90"))

    val personRDD: RDD[Person1] = userInfo.map(x => {
      val arr = x.split(" ")
      val name = arr(0)
      val age = arr(1).toInt
      val fv = arr(2).toInt

//      new Person1(name, age, fv)
      Person1(name, age, fv) // 样例类可以不用new来获取实例
    })

    // 排序
    val sorted: RDD[Person1] = personRDD.sortBy(x => x)

    println(sorted.collect.toBuffer)


    sc.stop()
  }
}

//// 普通类实现自定义排序, 需要实现Ordered特质, 需要序列化
//class Person1(val name: String, val age: Int, val fv: Int)
//  extends Serializable with Ordered[Person1] {
//  override def compare(that: Person1): Int = {
//    if (this.fv != that.fv) {
//      that.fv - this.fv
//    } else {
//      this.age - that.age
//    }
//  }
//
//  override def toString: String = s"$name, $age, $fv"
//}

// 样例类实现自定义排序，需要实现Ordered特质, 不需要序列化
case class Person1(val name: String, val age: Int, val fv: Int)
  extends Ordered[Person1] {
  override def compare(that: Person1): Int = {
    if (this.fv != that.fv) {
      that.fv - this.fv
    } else {
      this.age - that.age
    }
  }

  override def toString: String = s"$name, $age, $fv"
}