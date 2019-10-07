package com.qf.gp1922.day03

/**
  * 构造器：
  * 主构造器的声明方式：在主类名后面加参数列表
  * 主构造器的参数列表里可以放多个参数，这些参数往往作为初始化
  * 构造参数中，val修饰不可改变，var修饰可以后期更改其值
  * faceValue并没有用val或var修饰，默认就是val，不可更改
  * faceValue构造参数只能在本类访问，伴生对象也无法访问
  */
class StructDemo(val name: String, var age: Int, faceValue: Int = 60) {

  var gender: String = _
  var adress: String = _

  // 无法更改其值，默认为val
//  faceValue = 80

  def getFV = faceValue

  // 辅助构造器，可以引入主构造器的部分字段
  def this(name: String, age: Int, gender: String) {
    // 辅助构造器方法的第一行代码必须先调用主构造器的字段
    this(name, age)
    this.gender = gender
  }

  def this(name: String, age: Int, gender: String, adress: String) {
    // 辅助构造器方法的第一行代码必须先调用主构造器的字段
    this(name, age)
    this.adress = adress
  }

}
object StructDemo {
  def main(args: Array[String]): Unit = {
//    val sd = new StructDemo("huahua", 19)
    val sd = new StructDemo("huahua", 19, "male")
    println(sd.name)
    println(sd.gender)
    println(sd.getFV)
  }
}
