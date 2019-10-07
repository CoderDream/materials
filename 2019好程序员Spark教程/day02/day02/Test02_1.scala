package com.qf.gp1922.day02

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Test02_1 {
  def main(args: Array[String]): Unit = {
//    val arr = Array(1, 2, 3, 4, 5, 6)
//    //
//    val mapedArr: Array[Int] = arr.map(x => x)
//
//    println(mapedArr.toBuffer)
//    mapedArr.foreach(println)
//
//    var i = 0
//    val a = "a"
//
//    val b: String = "b"
//
//    val Array(x, y, z) = Array("A", false, 12)
//    val (m, n, l) = ("A", false, 12)
//
//    val res = {
//      if (i > 0) 1
//      else if (i < 0) -1
//      else 0
//    }
//
//
//    println(s"a的值为：${a}fsdfd")
//
//    for (i <- arr) println(i)
//
//
//    while (i < arr.length) {
//      println(arr(i))
//      i += 1
//    }
//
//    val ab = ArrayBuffer[Int]()
//    ab.insert(0, -1)
//    ab -= 1

    val arr = Array(2,3,5,4,6,1)
    println(arr.sortBy(x => x).toBuffer)

    val arr1 = Array(("a", 3),("b", 1),("c", 2))
    val sortedArr = arr1.sortBy(_._2)
    val sortedArr1 = arr1.sortWith(_._2 > _._2)

    println(sortedArr1.toBuffer)


    val list1 = ListBuffer[Int]()
    val list = new ListBuffer[Int]

  }

  def m1(x: Int): Int = {

    // 方法中，会将最后一行代码的值作为返回，但是，如果是赋值的过程，是不能作为返回
    val sum = x * x
    sum
  }

}
