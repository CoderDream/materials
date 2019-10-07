package com.qf.kafkatest.kafkabase2

import com.qf.kafkatest.kafkabase2.utils.MyKafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 场景：Streaming宕机一段时间或数据峰值期间都会造成kafka数据积压，
  * 如果不对Streaming的批次间隔做限速处理，在批次数据中会拉取很多数据，这样会影响处理效率
  * 限速参数：spark.streaming.kafka.maxRatePerPartition  每秒每个分区的记录数
  */
object KafkaRate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      // 设置对每个kafka分区每秒读取100条数据
      // 比如，kafka的某个topic有3个partition，streaming的批次间隔为4秒
      // 这样每个batch处理的数据为：3*4*100=1200
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .setAppName("test").setMaster("local[2]")

    val processingInterval = 4
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topic = "mykafka"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")



    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))


    val groupName = "myspark"
    val messages = MyKafkaUtils.createMyDirectKafkaStream(
      ssc, kafkaParams, topicsSet, groupName)



    messages.foreachRDD((rdd,btime) => {
      if(!rdd.isEmpty()){
        println("==========================:" + rdd.count() )
        println("==========================btime:" + btime )
      }
      MyKafkaUtils.saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
