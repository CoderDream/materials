package com.qf.gp1922.day03

/**
  * 闭包：就是能够读取其他方法（函数）内部的方法（函数）
  */
object ClosureTest {
  def sum(f: Int => Int): (Int, Int) => Int = {
    def sumF(a: Int, b: Int): Int = {
      if (a > b) 0 else f(a) + sumF(a + 1, b)
    }
    sumF
  }

  def main(args: Array[String]): Unit = {
    def sumInts = sum(x => x)
    val res = sumInts(1, 2)
    println(res)
  }
}
