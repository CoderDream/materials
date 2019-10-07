package com.qf.gp1922.day02

object Test02_2 {
  def main(args: Array[String]): Unit = {

    //创建一个List
    val lst0 = List(3,2,4,6,5,7,9,8,1)

    //将lst0中每个元素乘以2后生成一个新的集合
    val list1 = lst0.map(x => x * 2)

    //将lst0中的偶数取出来生成一个新的集合
    val list2 = lst0.filter(x => x % 2 == 0)

    //将lst0排序后生成一个新的集合
    val list3 = lst0.sorted
    val list4 = lst0.sortBy(x => x)
    val list5 = lst0.sortWith((x, y) => x > y)

    //反转排序顺序
    val list6 = lst0.sorted.reverse

    //将lst0中的元素4个一组,类型为Iterator[List[Int]]
    val it: Iterator[List[Int]] = lst0.grouped(4)
//    println(it)

    //将Iterator转换成List
    val list7: List[List[Int]] = it.toList
//    println(list7)

    //将多个list压平成一个List
    val list8 = list7.reduce((x, y) => x ::: y)
//    println(list8)
    val list9 = list7.flatten
//    println(list9)

    val lines = List("hello tom hello jerry", "hello suke hello", " hello tom")
    // 每条字符串先按空格切分，再压平，生成一个个单词
//    val wordsArr = lines.map(x => x.split(" "))
//    val words = wordsArr.flatten

    // flatMap方法是map方法和flatten方法的结合，先执行map再执行flatten压平
    val words = lines.flatMap(x => x.split(" "))
//    println(words)














    // 并行计算求和
    val arr = Array(1,2,3,4,5,6,7,8,9,10)
    var i = 0


//    println(arr.sum)
//    arr.reduce((x, y) => x + y)
//    println(arr.reduce(_ + _))
//    println(arr.reduce(_ - _)) // 单线程操作

    // 并行和并发的区别

    // 按照特定的顺序
//    println(arr.reduceLeft(_ + _))
    // 调用reduceLeft，不管有没有调用par方法，永远是单线程操作，而且会按照从左到右的顺序进行执行
//    println(arr.par.reduceLeft(_ - _))
    // 多线程累加
//    println(arr.par.reduce(_ - _)) // 多线程操作
//    while (true) {
//      i += 1
//      println(arr.par.reduce(_ - _))
//    }


    // 折叠(fold)：有初始值（无特定顺序）
    // (z: A1)(op: (A1, A1) => A1): A1
//    println(arr.fold(0)(_ + _))
//    println(arr.fold(10)(_ + _)) // 单线程计算
//    println(arr.par.fold(10)(_ + _)) // 多线程计算，在每个线程聚合的时候都会将初始值加一遍

    // 折叠：有初始值（有特定顺序）
//    println(arr.foldLeft(10)(_ + _))
//    println(arr.par.foldLeft(10)(_ + _)) // 当调用特定顺序的方法时，此时永远是单线程操作


    // 聚合
    val arr1 = List(List(1, 2, 3), List(3, 4, 5), List(2), List(0))
//    println(arr1.reduce((x, y) => List(x.sum + y.sum)))
//    println(arr1.flatten.sum)

    // aggregate方法实现了并行计算
    // 第一个参数列表代表初始值
    // 第二个参数列表的第一个参数：局部聚合（线程内聚合）
    // 第二个参数列表的第二个参数：全局聚合（线程和线程之间的聚合）
    // (z: =>B)(seqop: (B, A) => B, combop: (B, B) => B): B
    // 第一次循环，x代表拿到的是值，y代表拿到的是List。
    // 第二次循环，x代表拿到的是上次计算的结果，y代表拿到的是另一个List
//    arr1.aggregate(0)((x, y) => x + y.sum, (a, b) => a + b)
//    println(arr1.aggregate(0)(_ + _.sum, _ + _))


    // 求聚合值,结果为一个元组：（arr2的总和, 参与计算的数的个数） == （45，9）
    val arr2 = Array(1,2,3,4,5,6,7,8,9)
    println(arr2.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (m, n) => (m._1 + n._1, m._2 + n._2)))

    val l1 = List(5,6,4,7)
    val l2 = List(1,2,3,4)

    // 求并集
    println(l1 ++ l2)
    println(l1 union l2)


    // 求交集
    println(l1 intersect l2)


    // 求差集
    println(l1 diff l2)



  }
}
