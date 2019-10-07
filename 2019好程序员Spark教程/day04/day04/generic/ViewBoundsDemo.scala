package com.qf.gp1922.day04.generic

/**
  * 视界
  */
class ViewBoundsDemo[T <% Ordered[T]] {
  def chooser(first: T, second: T): T = {
    if (first > second) first else second
  }
}

object ViewBoundsDemo {
  def main(args: Array[String]): Unit = {
    import com.qf.gp1922.day04.MyPredef.girlSelect
    val chooser = new ViewBoundsDemo[MyGirl]

    val g1 = new MyGirl("mimi", 95, 31)
    val g2 = new MyGirl("yuanyuan", 95, 28)
    val girl = chooser.chooser(g1, g2)

    println(girl.name)
  }
}

class MyGirl(val name: String, val faceValue: Int, val age: Int){}