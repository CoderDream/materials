package com.qf.gp1922.day04

object CurryingDemo {
//  implicit val a = "tingjie"
//  implicit val b = "xiuxiu"
//  implicit val n = 100

  def currying(str: String)(implicit name: String = "huahua"): String = {
    str + name
  }

  def main(args: Array[String]): Unit = {
    import MyPredef.a
    println(currying("Hi~"))

  }
}
