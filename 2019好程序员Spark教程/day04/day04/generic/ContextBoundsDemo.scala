package com.qf.gp1922.day04.generic

/**
  * 上下文界定
  */
class ContextBoundsDemo[T: Ordering] {
  def chooser(first: T, second: T): T = {
    val ord: Ordering[T] = implicitly[Ordering[T]]
    if (ord.gt(first, second)) first else second
  }
}

object ContextBoundsDemo {
  def main(args: Array[String]): Unit = {
    import com.qf.gp1922.day04.MyPredef.OrderingGirl
    val chooser = new ContextBoundsDemo[MyGirl]

    val g1 = new MyGirl("ruhua", 40, 35)
    val g2 = new MyGirl("xiaoyueyue", 50, 24)

    val girl = chooser.chooser(g1, g2)

    println(girl.name)
  }
}
