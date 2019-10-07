package com.qf.gp1922.day04.generic

/**
  * 上界
  */
class UpperBoundsDemo[T <: Comparable[T]] {
  // 比较的方法
  def chooser(first: T, second: T): T = {
    if (first.compareTo(second) > 0) first else second
  }
}
object Chooser {
  def main(args: Array[String]): Unit = {
    val chooser = new UpperBoundsDemo[Girl]

    val g1 = new Girl("mimi", 95, 31)
    val g2 = new Girl("bingbing", 95, 30)

    val girl = chooser.chooser(g1, g2)

    println(girl.name)
  }
}

class Girl(val name: String, val faceValue: Int, val age: Int) extends Comparable[Girl]{
  override def compareTo(that: Girl): Int = {
    if (this.faceValue != that.faceValue) {
      this.faceValue - that.faceValue
    } else {
      that.age - this.age
    }
  }
}

