package com.qf.gp1922.day03

import scala.collection.mutable.ArrayBuffer

/**
  * 单例对象不能给构造器
  * 在创建单例对象的实例时，不用new
  * 在实例化单例对象时，里面的变量会赋值，方法会被声明
  */
object SingletonDemo {
  def main(args: Array[String]): Unit = {
    val s = SessionFactory

    println(s.getSession)
    println(s.getSession.size)

    println(s.removeSession())

    println(s.getSession)
    println(s.getSession.size)


  }
}

object SessionFactory{
  println("SessionFactory被执行了")

  // 计数器，用来创建对象
  var i = 5
  // 用于存放Session对象的变长数组
  val sessions = new ArrayBuffer[Session]()
  // 循环创建多个对象
  while (i > 0) {
    println("while循环被执行")
    sessions.append(new Session)
    i -= 1
  }

  // 获取Session对象的方法
  def getSession = sessions

  // 删除Session对象的方法
  def removeSession() = {
    val session = sessions(0)
    sessions.remove(0)
    println("Session对象被移除：" + session)
  }


}

class Session{}
