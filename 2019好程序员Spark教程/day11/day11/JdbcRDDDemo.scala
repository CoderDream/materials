package com.qf.gp1922.day11

import java.sql.{Date, DriverManager}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

//spark提供JDBCRDD
object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCRDDTest").setMaster("local")
    val sc = new SparkContext(conf)
    //插入的sql语句
    val sql = "select id,location,counts,access_date from location_info where id >= ? and id <= ?"
    //jdbc连接驱动设置
    val jdbcurl = "jdbc:mysql://node03:3306/bigdata?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "root"
    //获取连接
    val conn = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection(jdbcurl, user, password)
    }
    /*
    1.获取哦sparkContext对象
2. Connection连接对象
3.查询sql语句
4和5是获取数据的范围
6. partition个数
7.最终结果的返回
*/
    val jdbcRDD: JdbcRDD[(Int, String, Int, Date)] = new JdbcRDD(
      sc, conn, sql, 0, 200, 1,
      res => {
        //res查询之后获得的结果
        //通过get方法的重载形式传入不同 类名 得到数据
        val id = res.getInt("id")
        val location = res.getString("location")
        val counts = res.getInt("counts")
        val access_date = res.getDate("access_date")
        //JdbcRDD最终要返回这个查询的结果
        (id, location, counts, access_date)
      })

    println(jdbcRDD.collect.toList)

    sc.stop()
  }
}
