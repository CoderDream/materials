package com.qf.gp1922.day04

import scala.io.Source

class RichFile(val file: String) {
  def read(): String = {
    Source.fromFile(file).mkString
  }

}
object RichFile {
  def main(args: Array[String]): Unit = {

    val file = "c://data/file.txt"

    // 显式的实现对file的方法增强
//    val richFile = new RichFile(file)
//    val content: String = richFile.read()
//    println(content)

    // 隐式的对file做方法增强，用到了隐式转换函数
    import MyPredef.fileToRichFile
    val content: String = file.read()
    println(content)
  }
}


