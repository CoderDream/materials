package com.qf.gp1922.day02

object ScalaWordCount {
  def main(args: Array[String]): Unit = {

    val lines = List("hello tom hello jerry", "hello suke hello", " hello tom")

    // 将数据切分，生成一个个单词
    val words: List[String] = lines.flatMap(_.split(" "))

    // 将空字符串进行过滤
    val filtered: List[String] = words.filter(!_.isEmpty)

    // 将每个单词生成一个元组：(word, 1)
//    words.map(x => (x, 1))
    val tuples: List[(String, Int)] = filtered.map((_, 1))

    // 按照相同的单词进行分组
    val grouped: Map[String, List[(String, Int)]] = tuples.groupBy(_._1)
//    println(grouped)

    // 将相同单词的数据进行个数统计
//    val sumed: Map[String, Int] = grouped.map(x => (x._1, x._2.size))
    val sumed: Map[String, Int] = grouped.mapValues(_.size)
//    println(sumed)

    // 降序排序
    // Map是没有提供排序的方法的，可以将Map转换为List再排序
    val sorted: List[(String, Int)] = sumed.toList.sortBy(_._2).reverse
    println(sorted)


  }
}
