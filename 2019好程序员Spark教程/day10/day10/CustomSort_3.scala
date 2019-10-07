package com.qf.gp1922.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第三种方式：用隐式转换的方式实现自定义排序
  * 在实现的过程中，自定义类型只是起到一个封装值的作用，可以不实现Ordered特质
  * 隐式转换过程中，优先顺序为：隐式Object、隐式值、隐式函数、隐式方法
  * 隐式转换可以放到另外的独立的类中，使用时，需要import
  */
object CustomSort_3 {
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

        // 隐式的Object
        implicit object ordObject extends Ordering[Person3] {
          override def compare(x: Person3, y: Person3): Int = {
            if (x.fv == y.fv) {
              x.age - y.age
            } else {
              y.fv - x.fv
            }
          }
        }

    // 隐式方法
    implicit def orderedMethod(p: Person3): Ordered[Person3] = new Ordered[Person3]{
      override def compare(that: Person3) = {
        if (p.fv == that.fv) {
          p.age - that.age
        } else {
          that.fv - p.fv
        }
      }
    }

    // 隐式函数
    implicit val orderedFunc = (p: Person3) => new Ordered[Person3] {
      override def compare(that: Person3) = {
        if (p.fv == that.fv) {
          p.age - that.age
        } else {
          that.fv - p.fv
        }
      }
    }

    // 隐式值
    implicit val ordVar: Ordering[Person3] = new Ordering[Person3] {
      override def compare(x: Person3, y: Person3) = {
        if (x.fv == y.fv) {
          x.age - y.age
        } else {
          y.fv - x.fv
        }
      }
    }

    // 排序
    val sorted: RDD[(String, Int, Int)] = personRDD.sortBy(x => Person3(x._1, x._2, x._3))

    println(sorted.collect.toBuffer)



    sc.stop()
  }
}

case class Person3(val name: String, val age: Int, val fv: Int) {
  override def toString: String = s"$name, $age, $fv"
}

