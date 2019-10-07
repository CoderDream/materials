package com.qf.kafkatest.kafkabase1

/**
  * 用checkpoint记录offset
  * 优点：实现过程简单
  * 缺点：如果streaming的业务更改，或别的作业也需要获取该offset，是获取不到的
  */
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object StreamingWithCheckpoint {
  def main(args: Array[String]) {
    //val Array(brokers, topics) = args
    val processingInterval = 2
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topics = "mytest1"
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("ConsumerWithCheckPoint").setMaster("local[2]")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val checkpointPath = "hdfs://node01:9000/spark_checkpoint1"

    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

      ssc.checkpoint(checkpointPath)
      messages.checkpoint(Duration(8 * processingInterval.toInt * 1000))
      messages.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          println("################################" + rdd.count())
        }

      })
      ssc
    }

    // 如果没有checkpoint信息，则新建一个StreamingContext
    // 如果有checkpoint信息，则从checkpoint中记录的信息恢复StreamingContext
    // createOnError参数：如果在读取检查点数据时出错，是否创建新的流上下文。
    // 默认情况下，将在错误上引发异常。
    val context = StreamingContext.getOrCreate(checkpointPath, functionToCreateContext _)
    context.start()
    context.awaitTermination()
  }
}
