package com.qf.gp1922.day03

/**
  * 类名和类文件名称可以不统计
  * 一个类文件里可以有多个类
  * 声明一个类，不用加public等关键字，默认就是public
  * 声明属性的时候，可以不用get、set方法，利用var和val的特性就可以实现
  * 用“_”给初始值时，变量必须要用var修饰，而且必须手动给类型，如果Int类型，初始值为0，如果String类型，初始值为null
  */
class Person {
  // 用var修饰字段相当于有get方法，也有set方法
  var id: Int = _

  var name: String = _

  // 用val修饰字段相当于只有get方法，没有set方法
  val adress: String = "huilongguan"

  // 该字段前面加private，称为私有字段，只能在本类和其伴生对象中访问
  private var age: Int = _

  // 用private [this]修饰的字段称为对象私有字段，只能在本类访问，伴生对象也访问不到
  private [this] val gender = "male"

  // 如果想访问对象私有字段，一般我们会声明一个获取对象私有字段的方法
  def getGender = gender

}
object Person{
  def main(args: Array[String]): Unit = {
    val p = new Person
    p.id = 1
    p.name = "huahua"
    println(p.id)
    println(p.name)
    p.age = 19
    println(p.age)
//    p.gender // 对象私有字段无法访问
    println(p.getGender)
  }
}

object PersonTest {
  def main(args: Array[String]): Unit = {
    val p = new Person
//    p.age // 私有字段是无法访问的
    p.getGender
  }
}

