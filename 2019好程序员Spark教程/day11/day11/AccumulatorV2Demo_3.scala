package com.qf.gp1922.day11

import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 自定义Accumulator实现单词计数
  */
object AccumulatorV2Demo_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val fileInfo = sc.textFile("c://data/file.txt", 2)

    // 1、获取自定义Accumulator累计器的实例
    val accmulator = new MyAccumulator_wc()

    // 2、注册
    sc.register(accmulator, "wc")

    // 3、进行累加
    fileInfo.foreach(line => accmulator.add(line))

    println(accmulator.value)

    sc.stop()
  }
}

/**
  * AccumulatorV2[IN, OUT]: 需要定义输入类型和输出类型
  */
class MyAccumulator_wc extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  // 初始值
  private val accMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = accMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new MyAccumulator_wc
    accMap.synchronized{
      acc.accMap ++= accMap
    }

    acc
  }

  override def reset(): Unit = accMap.clear()

  override def add(v: String): Unit = {
    val splited = v.split(" ")
    splited.map(word => {
      accMap.get(word) match {
        case Some(x) => accMap += ((word, x + 1))
        case None => accMap += ((word, 1))
      }
    })

  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case a: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
        for ((k, v) <- a.value) {
          accMap.get(k) match {
            case Some(x) => accMap += ((k, x + v))
            case None => accMap += ((k, v))
          }
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = accMap
}
