package com.qf.kafkatest.kafkabase1

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

object KafkaZKManager  extends Serializable{
  /**
    * 创建rookeeper客户端
    */
  val client = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString("node01:2181/kafka0.9") // zk中kafka的路径
      .retryPolicy(new ExponentialBackoffRetry(1000, 3)) // 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加
      .namespace("mykafka") // 命名空间:mykafka
      .build()
    client.start()
    client
  }

  val kafkaOffsetRootPath = "/consumers/offsets"

  /**
    * 确保zookeeper中的路径是存在的
    * @param path
    */
  def ensureZKPathExists(path: String): Unit = {
    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  def storeOffsets(offsetsRanges:Array[OffsetRange], groupName:String) = {
    for (o <- offsetsRanges) {
      val zkPath = s"${kafkaOffsetRootPath}/${groupName}/${o.topic}/${o.partition}"
      ensureZKPathExists(zkPath)
      // 保存offset到zk
      client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }

  /**
    * 用于获取offset
    * @param topic
    * @param groupName
    * @return
    */
  def getFromOffsets(topic : String,groupName : String): (Map[TopicAndPartition, Long], Int) = {
    // 如果 zookeeper中有保存offset,我们会利用这个offset作为kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val zkTopicPath = s"${kafkaOffsetRootPath}/${groupName}/${topic}"
    // 确保zookeeper中的路径是否存在
    ensureZKPathExists(zkTopicPath)
    // 获取topic中，各分区对应的offset
    val offsets: mutable.Buffer[(TopicAndPartition, Long)] = for {
    // 获取分区
      p <- client.getChildren.forPath(zkTopicPath)
    } yield {
      //遍历路径下面的partition中的offset
      val data = client.getData.forPath(s"$zkTopicPath/$p")
      //将data变成Long类型
      val offset = java.lang.Long.valueOf(new String(data)).toLong
      println("offset:" + offset)
      (TopicAndPartition(topic, Integer.parseInt(p)), offset)
    }
    offsets

    if(offsets.isEmpty) {
      (offsets.toMap,0)
    }else{
      (offsets.toMap,1)
    }
  }

  def main(args: Array[String]): Unit = {
    val processingInterval = 2
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topic = "mytest1"
    val sparkConf = new SparkConf().setAppName("KafkaZKManager").setMaster("local[2]")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    // 读取kafka数据
    val messages = createMyDirectKafkaStream(ssc, kafkaParams, topic, "group01")

    messages.foreachRDD((rdd,btime) => {
      if(!rdd.isEmpty()){
        println("==========================:" + rdd.count() )
        println("==========================btime:" + btime )
      }
      // 消费到数据后，将offset保存到zk
      storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, "group01")
    })

    ssc.start()
    ssc.awaitTermination()
   }

  def createMyDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], topic: String, groupName: String): InputDStream[(String, String)] = {
    // 获取offset
    val (fromOffsets, flag) = getFromOffsets( topic, groupName)
    var kafkaStream : InputDStream[(String, String)] = null
    if (flag == 1) {
      // 这个会将kafka的消息进行transform,最终kafak的数据都会变成(topic_name, message)这样的tuple
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      println("fromOffsets:" + fromOffsets)
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      // 如果未保存,根据kafkaParam的配置使用最新或者最旧的offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet)
    }
    kafkaStream
  }

}
