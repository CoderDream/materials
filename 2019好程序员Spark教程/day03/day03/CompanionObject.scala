package com.qf.gp1922.day03

/**
  * 和主类名相同的，用object修饰的类称作主类的伴生对象
  * 单例对象包含伴生对象
  * 主类和他的伴生对象可以互相访问私有字段和属性
  */
class CompanionObject {
  var id = 0
  private val name = "tingjie"
  def printContent() = {
    println(name + CompanionObject.content)
  }
}
object CompanionObject {
  private val content = "is my goddess"

  def main(args: Array[String]): Unit = {
    val co = new CompanionObject
    co.id = 10
    println(co.id)
    println(co.name)
    co.printContent()
  }
}
