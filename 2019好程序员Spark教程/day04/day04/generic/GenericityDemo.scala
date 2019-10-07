package com.qf.gp1922.day04.generic

/**
  * 需求：有一个自定义对象，生成多个实例，比较实例里的某个字段
  */
class GenericityDemo {

}
// 给具体的比较规则
class Teacher(val name: String, val faceValue: Int) extends Comparable[Teacher]{
  override def compareTo(that: Teacher): Int = this.faceValue - that.faceValue
}
object Teacher{
  def main(args: Array[String]): Unit = {
    val t1 = new Teacher("dazhao", 95)
    val t2 = new Teacher("xiaodong", 990)

    val arr = Array(t1, t2)

    val sorted: Array[Teacher] = arr.sorted.reverse

    println(sorted(0).name)
  }
}


