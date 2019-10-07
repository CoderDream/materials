package com.qf.gp1922.day02

class ScalaLazyDemo {

}
object ScalaLazyDemo1 {
  def init(): Unit = {
    println("init")
  }



  def main(args: Array[String]): Unit = {
    val prop = init() // 没有用lazy修饰符修饰的变量

    println("after init()")

    println(prop)

  }
}

object ScalaLazyDemo2 {
  def init(): Unit = {
    println("init")
  }

  def main(args: Array[String]): Unit = {
    lazy val prop = init() // 没有用lazy修饰符修饰的变量

    println("after init()")

    println(prop)

  }
}