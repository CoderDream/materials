package com.qf.gp1922.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第二种方式：可以用普通类或样例类来指定排序规则
  * 在调用sortBy传参的时候，需要将数据封装到自定义排序类中，这样就相当于指定了排序规则了
  */
object CustomSort_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val userInfo = sc.parallelize(Array("mimi 28 85", "bingbing 30 85", "yuanyuan 18 90"))

    val personRDD: RDD[(String, Int, Int)] = userInfo.map(x => {
      val arr = x.split(" ")
      val name = arr(0)
      val age = arr(1).toInt
      val fv = arr(2).toInt

      (name, age, fv)
    })

    // 排序
//    val sorted: RDD[(String, Int, Int)] = personRDD.sortBy(x => new Person2(x._1, x._2, x._3))
    val sorted: RDD[(String, Int, Int)] = personRDD.sortBy(x => Person2(x._1, x._2, x._3))

    println(sorted.collect.toBuffer)


    sc.stop()
  }
}

// 普通类实现自定义排序, 需要实现Ordered特质, 需要序列化
//class Person2(val name: String, val age: Int, val fv: Int)
//  extends Serializable with Ordered[Person2] {
//  override def compare(that: Person2): Int = {
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
case class Person2(val name: String, val age: Int, val fv: Int)
  extends Ordered[Person2] {
  override def compare(that: Person2): Int = {
    if (this.fv != that.fv) {
      that.fv - this.fv
    } else {
      this.age - that.age
    }
  }

  override def toString: String = s"$name, $age, $fv"
}