package com.qf.gp1922.day03.matchdemo

/**
  * Some类型可以封装多类型的数据，Some的父类是Option
  */
object OptionDemo {
  def main(args: Array[String]): Unit = {
//    val seq: Option[(String, Int, Boolean)] = Some("huahua", 18, true)
//    val value: (String, Int, Boolean) = seq.getOrElse(null)
//    println(value)

    val map = Map(("tingjie", 19),("huahua", 18),("xiuxiu", 24))

    val value = map.get("tingjie")

    value match {
      case Some(x) => println(s"case Some, x: $x")
      case None => println("case None")
    }

  }
}
