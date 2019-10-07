package com.qf.gp1922.day03.matchdemo

import scala.util.Random

object MatchString {
  def main(args: Array[String]): Unit = {

    val arr = Array("zhoudongyu", "yangzi", "guanxiaotong", "zhengshuang")

    val name = arr(Random.nextInt(arr.length))

    println(name)

    name match {
      case "zhoudongyu" => println("周冬雨")
      case "yangzi" => println("杨紫")
      case "guanxiaotong" => println("关晓彤")
      case "zhengshuang" => println("郑爽")
    }



  }
}
