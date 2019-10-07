package com.qf.gp1922.day04

import com.qf.gp1922.day04.generic.MyGirl

object MyPredef {
  implicit val a = "tingjie"
  implicit val b = "xiuxiu"
  implicit val n = 100

//  implicit val fileToRichFile = (file: String) => new RichFile(file)
  implicit def fileToRichFile(file: String) = new RichFile(file)

  implicit val girlSelect = (girl: MyGirl) => new Ordered[MyGirl] {
    override def compare(that: MyGirl) = {
      if (girl.faceValue == that.faceValue)
        that.age - girl.age
      else
        girl.faceValue - that.faceValue
    }
  }

  implicit object OrderingGirl extends Ordering[MyGirl] {
    override def compare(x: MyGirl, y: MyGirl): Int = {
      if (x.faceValue == y.faceValue)
        y.age - x.age
      else
        x.faceValue - y.faceValue
    }
  }
}
