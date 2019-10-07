package com.qf.kafkatest.kafkabase2.utils

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._

object MyKafkaUtils {
  // 在zookeeper中, kafka的offset保存的根目录
  val kakfaOffsetRootPath = "/consumers/offsets"

  // 初始化Zookeeper客户端
  val zkClient = {
    val client = CuratorFrameworkFactory.builder.connectString("spark1234:12181/kafka0.9").
      retryPolicy(new ExponentialBackoffRetry(1000, 3)).namespace("mykafka").build()

    client.start()
    client
  }

  /**
    * 判断zookeeper的路径是否存在, 如果不存在则创建
    * @param path  zookeeper的目录路径
    */
  def ensureZKPathExists(path: String): Unit = {
    if (zkClient.checkExists().forPath(path) == null) {
      zkClient.create().creatingParentsIfNeeded().forPath(path)
    }
  }


  def saveOffsets(offsetsRanges:Array[OffsetRange], groupName:String) = {
    for (o <- offsetsRanges) {
      val zkPath = s"${kakfaOffsetRootPath}/${groupName}/${o.topic}/${o.partition}"
      ensureZKPathExists(zkPath)
      zkClient.setData().forPath(zkPath,o.untilOffset.toString.getBytes())
    }
  }


  // ${kakfaOffsetRootPath}/${groupName}/${topic}
  def getZKOffsets(topicSet:Set[String], groupName:String, kafkaParam: Map[String, String]) : Map[TopicAndPartition, Long] = {

    var offsets: Map[TopicAndPartition, Long] = Map()


    val offGroupPath = kakfaOffsetRootPath + "/" + groupName
    // 如果路径不存在， 则offset没有保存
    if (zkClient.checkExists().forPath(offGroupPath) == null) {
      return offsets
    }

    offsets = getResetOffsets(kafkaParam, topicSet )

    for{
      topic<-zkClient.getChildren.forPath(offGroupPath)
      if (topicSet.contains(topic))
      partition <- zkClient.getChildren.forPath(offGroupPath + "/" + topic)
    }yield {
      val partionPath = offGroupPath + "/" + topic + "/" + partition
      val offset =  zkClient.getData.forPath(partionPath) // if (zkClient.checkExists().forPath(partionPath) != null) zkClient.getData.forPath(partionPath) else "-1"
      offsets += TopicAndPartition(topic, Integer.parseInt(partition)) -> java.lang.Long.valueOf(new String(offset)).toLong
    }

    offsets
  }

  /**
    *
    * @param kafkaParam
    * @param topicSet
    * @param groupName
    * @return
    */
  def getConSumerOffsets(kafkaParam: Map[String, String], topicSet:Set[String], groupName:String) : Map[TopicAndPartition, Long] = {

    val brokers = kafkaParam("metadata.broker.list")

    val kafkaSmallestParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val kafkaLargestParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")


    var offsets: Map[TopicAndPartition, Long] = Map()

    val smallOffsets = getResetOffsets(kafkaSmallestParams, topicSet)
    val largestOffsets = getResetOffsets(kafkaLargestParams, topicSet)

    val consumerOffsets = getZKOffsets(topicSet, groupName, kafkaParam) // cOffset-从外部存储中读取的offset


    smallOffsets.foreach({
      case(tp, sOffset) => {
        val cOffset = if (!consumerOffsets.containsKey(tp)) 0 else  consumerOffsets(tp)
        val lOffset = largestOffsets(tp)
        if(sOffset > cOffset) {
          offsets += tp->sOffset
        } else if(cOffset > lOffset){
          offsets += tp->lOffset
        } else{
          offsets += tp->cOffset
        }
      }
    })

    offsets
  }



  /**
    * 获取smallest或者largest的offset
    * @param kafkaParam
    * @param topics     topic集合, 多个topic使用逗号分隔
    * @return
    */
  def getResetOffsets(kafkaParam: Map[String, String], topics: Set[String]): Map[TopicAndPartition, Long] = {

    val cluster = new MyKafkaCluster(kafkaParam)

    var offsets: Map[TopicAndPartition, Long] = Map()

    // 最新或者最小offset  reset为smallest或largest
    val reset = kafkaParam.get("auto.offset.reset").map(x => x.toLowerCase())
    val topicAndPartitions: Set[TopicAndPartition] = cluster.getPartitions(topics).right.get


    if (reset == Some("smallest")) {
      val leaderOffsets = cluster.getEarliestLeaderOffsets(topicAndPartitions).right.get
      topicAndPartitions.foreach(tp => {
        offsets += tp -> leaderOffsets(tp).offset
      })
    } else if (reset == Some("largest")) {
      val leaderOffsets = cluster.getLatestLeaderOffsets(topicAndPartitions).right.get
      topicAndPartitions.foreach(tp => {
        offsets += tp -> leaderOffsets(tp).offset
      })
    }
    offsets
  }

  def createMyDirectKafkaStream (ssc: StreamingContext,kafkaParams: Map[String, String], topics: Set[String], groupName: String
                                ): InputDStream[(String, String)] = {

    val fromOffsets = getConSumerOffsets(kafkaParams, topics, groupName)
    var kafkaStream : InputDStream[(String, String)] = null

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    kafkaStream
  }



  def createNewDirectKafkaStream (ssc: StreamingContext,kafkaParams: Map[String, String], topics: Set[String], groupName: String
                                ): InputDStream[(String, Int, Long, String)] = {

    val fromOffsets = getConSumerOffsets(kafkaParams, topics, groupName)
    var kafkaStream : InputDStream[(String, Int, Long, String)] = null

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.partition, mmd.offset, mmd.message())
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Int, Long, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    kafkaStream
  }



  def createNewDirectKafkaStream1 (ssc: StreamingContext,kafkaParams: Map[String, String], topics: Set[String], groupName: String
                                ): InputDStream[(String, Int, Long, String)] = {

    val fromOffsets = getConSumerOffsets(kafkaParams, topics, groupName)
    //println("fromOffsets==" + fromOffsets)
    var kafkaStream : InputDStream[(String, Int, Long, String)] = null

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.partition, mmd.offset, mmd.message())
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Int, Long, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    kafkaStream
  }



  def main(args: Array[String]): Unit = {
    val brokers = "spark1234:9092"
    val topic = "mykafka" //
    val topicsSet = topic.split(",").toSet

    // 获取topic中有效的最小offset
    val kafkaParamsSmallest = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val smallestOffsets = getResetOffsets(kafkaParamsSmallest, topicsSet)

    // 获取topic中有效的最新offset
    val kafkaParamsLargest = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    val largestOffsets = getResetOffsets(kafkaParamsLargest, topicsSet)

    // 打印
    println("========Smallest offsets=============:" + smallestOffsets)
    println("========Largest offsets=============:" + largestOffsets)

    //println(getZKOffsets(Set("dd,mytest1"), "abc"))
  }
}
