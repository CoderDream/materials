package com.qf.kafkatest.kafkabase2.semantic

import java.sql.DriverManager

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.qf.kafkatest.kafkabase2.utils.MyKafkaUtils

object KafkaOffsetApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    val processingInterval = 2
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topic = "mykafka"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))


    val groupName =  "myspark"
    val messages = MyKafkaUtils.createMyDirectKafkaStream(ssc, kafkaParams, topicsSet, groupName)

    val jdbcUrl =  "jdbc:mysql://node03:3306/myspark"
    val jdbcUser = "root"
    val jdbcPassword = "root"

    messages.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(x=>x._2).foreachPartition(partition =>{

        val pOffsetRange = offsetRanges(TaskContext.get.partitionId)


        val sql = "insert into yourtest(name, id) values (?,?)"
        val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
        dbConn.setAutoCommit(false)

        val pstmt = dbConn.prepareStatement(sql)
        partition.foreach(msg=>{
          val name = msg.split(",")(0)
          val id = msg.split(",")(1)
          pstmt.setString(1, name)
          pstmt.setInt(2, Integer.parseInt(id))
          pstmt.addBatch()
        })

        val offsetSql = s"update mytopic set offset=${pOffsetRange.untilOffset} where topic='${pOffsetRange.topic}' and tpartition=${pOffsetRange.partition}"
        println("offsetSql"+offsetSql)
        val offsetPstmt = dbConn.prepareStatement(offsetSql)

        pstmt.executeBatch
        offsetPstmt.execute()
        dbConn.commit()
        pstmt.close()
        dbConn.close()
      })
    })

    messages.foreachRDD((rdd,btime) => {
      if(!rdd.isEmpty()){
        rdd.map(x=>x._2).foreach(println)
        println("==========================:" + rdd.count() )
        println("==========================btime:" + btime )
      }
      MyKafkaUtils.saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
