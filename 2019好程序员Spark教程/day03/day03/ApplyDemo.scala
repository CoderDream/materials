package com.qf.gp1922.day03

/**
  * apply方法只能声明在主类的伴生对象中
  * apply方法通常被称为注入方法
  * 用途：经常用apply方法做一些初始化的操作
  * apply方法的参数列表和主类的构造器的参数可以不统一
  * unapply方法通常被称为提取方法，使用unapply方法可以提取固定数量的值或对象
  * unapply方法会返回一个Option对象，如果Option有值，会返回一个Some对象来封装具体的值，如果没有值，会返回一个None对象
  * apply方法和unapply方法都是被隐式调用的
  */
class ApplyDemo(val name: String, val age: Int, val faceValue: Int = 60) {

}
object ApplyDemo {
  // 注入方法
  def apply(name: String, age: Int): ApplyDemo = {
    new ApplyDemo(name, age)
  }

  // 提取方法
  def unapply(applyDemo: ApplyDemo): Option[(String, Int, Int)] = {
    if (applyDemo == null) {
      None
    } else {
      Some(applyDemo.name, applyDemo.age, applyDemo.faceValue)
    }
  }

}

object ApplyTest{
  def main(args: Array[String]): Unit = {
    // 调用apply
    val applyDemo = ApplyDemo("xiaoli", 20)

    // 调用unapply
    applyDemo match {
      case ApplyDemo(name, age, faceValue) => println(s"name: $name, age: $age, facevalue: $faceValue")
      case _ => println("No match nothing")
    }


  }
}
