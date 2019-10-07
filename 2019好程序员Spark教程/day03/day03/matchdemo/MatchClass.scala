package com.qf.gp1922.day03.matchdemo

import scala.util.Random

class MatchClass {

}

object MatchClass{
  def main(args: Array[String]): Unit = {
    val matchClass = new MatchClass
    val arr = Array("tingjie", 18, true, matchClass)

    val ele = arr(Random.nextInt(arr.length))

    println(ele)

    ele match {
      case str: String => println("match String")
      case int: Int => println("match Int")
      case bool: Boolean => println("match Boolean")
      case matchClass: MatchClass => println("match MatchClass")
    }
  }
}
