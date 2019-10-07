package com.qf.gp1922.day10

object HashTest1 {
  def main(args: Array[String]): Unit = {
    val key = "android.learn.com"
    val numPartitons = 3

    val rawMod = key.hashCode % numPartitons
    val num = rawMod + (if (rawMod < 0) numPartitons else 0)

    println(num)
  }
}
