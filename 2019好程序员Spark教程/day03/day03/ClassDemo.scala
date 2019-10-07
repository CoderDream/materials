package com.qf.gp1922.day03

object ClassDemo {
  def main(args: Array[String]): Unit = {
    val h = new Human

    println(h.name)
    println(h.climb)
    println(h.fly)

  }
}

/**
  * 特质：trait
  */
trait Flyable {
  // 有值的字段
  val distance: Int = 100
  // 没有值的字段
  val hight: Int
  // 有方法体的方法
  def fly: String = "I can fly"
  // 没有方法体的方法
  def fight: String

}

/**
  * 抽象类
  */
abstract class Animal {
  // 有值的字段
  val age: Int = 19
  // 没有值的字段
  val name: String
  // 有方法体的方法
  def climb: String = "I can climb"
  // 没有方法体的方法
  def run
}

/**
  * 在没有继承父类的情况下，如果需要实现trait，此时的关键字为extends
  * 在有父类的情况下，关键字为with
  */
class Human extends Animal with Flyable {

  override val name: String = "dazhao"

  // 重写有方法体的方法
  override def climb: String = "I can not climb"

  override def run: Unit = "I can run"

  // 实现没有值的字段
  override val hight: Int = 2

  override def fly: String = "I can not fly"

  // 实现没有方法体的方法
  override def fight: String = "fighting with 棒子"
}
