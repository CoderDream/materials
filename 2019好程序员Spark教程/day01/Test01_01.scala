package com.qf.gp1922.day01

/**
  * test
  */
object Test01_01 {
  def main(args: Array[String]): Unit = {
    /*
    val str = "abc"
    val abc = "aaa"
    */

    //    println("hello scala")

    // 字符串引用变量值
    //    val name = "tingjie"
    //    val age = 18
    //
    //    val userInfo = s"name: ${name} age: ${age}"
    //    println(userInfo)

//    val prop = "key1=a,key2=b,key3=c"
    //    val str =
    //      """
    //        |key1=a
    //        |key2=b
    //        |key3=c
    //      """.stripMargin
    //    println(str)

    // Scala的输入和输出
//    m1()

    //高级for循环(嵌套循环加过滤功能)
//    for(i <- 1 to 3 ; j<- 1 to 3 if i != j)
//      print((10*i + j) + "")
//    println()
//    for (i <- 1 to 3) {
//      for (j <- 1 to 3) {
//        if (i != j) {
//          println((10*i + j) + "")
//        }
//      }
//    }

    // break、continue
    // break例子
//    import util.control.Breaks._
//    breakable( // breakable包含的哪个for循环，执行break()的时候就会跳出该循环
//      for(i<-0 until 10) {
//        if(i==5){
//          break()
//        }
//        println(i)
//      }
//    )

    // continue例子
//    for(i<-0 until 10) {
//      breakable{ // 在for循环的里面，代表跳出本次循环
//        if(i==5){
//          break()
//        }
//        println(i)
//      }
//    }

    // 声明方法时，可以不写方法的返回值，方法可以类型推断，但在有递归的时候，必须给返回值
    // 兔子数列
    println(m2(3))

  }


  // 模拟门店(游戏厅)的门禁
  def m1(): Unit = {
//    val name = readLine()
    val name = scala.io.StdIn.readLine("Welcome to Game House, Please tell me your name: \n")
    print("Thanks, Then please tell me your age: \n")
//    val age = readInt()
    val age = scala.io.StdIn.readInt()

    if (age > 18) {
      println(s"Hi~, $name, you are $age years old, so you are legal to come here! ")
    } else {
      println(s"Sorry, boy, $name, you are only $age years old.")
    }


  }

  def m2(n: Int): Int = {
    if (n == 1 || n == 2) 1
    else m2(n - 1) + m2(n - 2)
  }


}
