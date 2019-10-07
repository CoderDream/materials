package com.qf.gp1922.day03.matchdemo

import scala.util.Random

/**
  * 样例类就是用case class声明的类
  * 样例类分两种：case object 和 case class
  * 其中case object是没有构造参数的，case class是可以放构造参数的
  * 总结样例类的作用：
  *   1、和模式匹配搭配使用，使得代码更加灵活
  *   2、样例类case class可以封装多个值到样例类中，也就是起到封装的效果
  */
object CaseClassDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array(CheckTimeOutWorker, SubmitTask("001", "task_001"), HeatBeat(10000))

    arr(Random.nextInt(arr.length)) match {
      case CheckTimeOutWorker => println("CheckTimeOutWorker")
      case SubmitTask(id, task) => println("SubmitTask")
      case HeatBeat(time) => println("HeatBeat")
      case _ => println("nothing...")
    }

  }
}

case object CheckTimeOutWorker
case class SubmitTask(id: String, taskName: String)
case class HeatBeat(time: Long)
