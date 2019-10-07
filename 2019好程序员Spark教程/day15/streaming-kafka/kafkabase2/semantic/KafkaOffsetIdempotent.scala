package com.qf.kafkatest.kafkabase2.semantic

import java.sql.DriverManager

import com.qf.kafkatest.kafkabase2.utils.MyKafkaUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * 一次语义：幂等写入
  * 当获取到数据后，先写到mysql，再保存offset，
  * 如果在写到mysql数据后，在保存offset之前宕机，重启作业后也不会影响一次语义
  * 因为会在mysql重复更新
  */
object KafkaOffsetIdempotent {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    val processingInterval = 2
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topic = "mytopic1"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")

    /*
       1.创建测试的mysql数据库
       create database mytest;
       2.建表
       create table myorders(name varchar(100), orderid varchar(100) primary key);
       3.新建topic： mytopic1
         kafka-topics.sh --zookeeper node01:2181 --create --topic mytopic1 --partitions 3 --replication-factor 1
       4.往mytopic1发送数据，数据格式为 "name,orderid"  比如  abc,3
     */
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    val groupName =  "group1"
    val messages = MyKafkaUtils.createMyDirectKafkaStream(
      ssc, kafkaParams, topicsSet, groupName)

    val jdbcUrl =  "jdbc:mysql://node03:3306/mytest"
    val jdbcUser = "root"
    val jdbcPassword = "root"

    messages.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(x=>x._2).foreachPartition(partition =>{
        val conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)

        // upsert update insert
        partition.foreach(msg=>{
          val name = msg.split(",")(0)
          val orderid = msg.split(",")(1)
          // orderid为主键
          // 插入数据时，会找是否之前的数据有相同的orderid，如果有，就更新name，没有就插入
          // 这样就可以实现幂等写入
          val sql = s"insert into myorders(name, orderid) values ('$name', '$orderid') ON DUPLICATE KEY UPDATE name='${name}'"
          val pstmt = conn.prepareStatement(sql)
          pstmt.execute()
        })

        conn.close()
      })
      MyKafkaUtils.saveOffsets(offsetRanges, groupName)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
