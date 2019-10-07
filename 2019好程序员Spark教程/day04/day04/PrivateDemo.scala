package com.qf.gp1922.day04

/**
  * private加包名是指包访问权限
  * 构造器前加private，是指构造器的伴生对象访问权限，只有本类和其伴生对象才能访问
  */
private [day04] class PrivateDemo private (val name: String) {
  // 私有字段，只有本类和伴生对象有反问权限
  private val age: Int = 10
  // 对象私有字段，只能在本类访问
  private [this] val faceValue: Int = 100
}
