package com.qf.gp1707.day29.persistoffset

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaHbaseManager {
  // 保存offset到hbase
  def saveOffsets(TOPIC_NAME: String, GROUP_ID: String, offsetRanges: Array[OffsetRange],
                  hbaseTableName: String, batchTime: org.apache.spark.streaming.Time) = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val rowKey = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(batchTime.milliseconds)
    val put = new Put(rowKey.getBytes())
    for (offset <- offsetRanges) {
      put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString),
        Bytes.toBytes(offset.untilOffset.toString))
    }
    table.put(put)
    conn.close()
  }

  // 从zookeeper中获取topic的分区数
  def getNumberOfPartitionsForTopicFromZK(TOPIC_NAME: String, GROUP_ID: String,
                                          zkQuorum: String, zkRootDir: String, sessTimeout: Int, connTimeOut: Int): Int = {
    val zkUrl = zkQuorum + "/" + zkRootDir
    val zkClientAndConn = ZkUtils.createZkClientAndConnection(zkUrl, sessTimeout, connTimeOut)
    val zkUtils = new ZkUtils(zkClientAndConn._1, zkClientAndConn._2, false)
    // 获取分区数量
    val zkPartitions = zkUtils.getPartitionsForTopics(Seq(TOPIC_NAME)).get(TOPIC_NAME).toList.head.size
    println(zkPartitions)
    zkClientAndConn._1.close()
    zkClientAndConn._2.close()
    zkPartitions
  }

  // 获取hbase的offset
  def getLastestOffsets(TOPIC_NAME: String, GROUP_ID: String, hTableName: String,
                        zkQuorum: String, zkRootDir: String, sessTimeout: Int, connTimeOut: Int): Map[TopicAndPartition, Long] = {

    // 连接zk获取topic的partition数量
    val zKNumberOfPartitions = getNumberOfPartitionsForTopicFromZK(TOPIC_NAME, GROUP_ID, zkQuorum, zkRootDir, sessTimeout, connTimeOut)

    val hbaseConf = HBaseConfiguration.create()

    // 获取hbase中最后提交的offset
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hTableName))
    val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
    val scan = new Scan()
    val scanner = table.getScanner(scan.setStartRow(startRow.getBytes).setStopRow(stopRow.getBytes).setReversed(true))
    val result = scanner.next()

    var hbaseNumberOfPartitions = 0 // 在hbase中获取的分区数量
    if (result != null) {
      // 将分区数量设置为hbase表的列数量
      hbaseNumberOfPartitions = result.listCells().size()
    }

    val fromOffsets = collection.mutable.Map[TopicAndPartition, Long]()
    if (hbaseNumberOfPartitions == 0) { // 如果没有保存过offset
      // 初始化kafka为开始
      for (partition <- 0 until zKNumberOfPartitions) {
        fromOffsets += ((TopicAndPartition(TOPIC_NAME, partition), 0))
      }

    } else if (zKNumberOfPartitions > hbaseNumberOfPartitions) { // 如果zk的partition数量大于hbase的partition数量，说明topic增加了分区，就需要对分区做单独处理
      // 处理新增加的分区添加到kafka的topic
      for (partition <- 0 until zKNumberOfPartitions) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += ((TopicAndPartition(TOPIC_NAME, partition), fromOffset.toLong))
      }
      // 对新增加的分区将它的offset值设为0
      for (partition <- hbaseNumberOfPartitions until zKNumberOfPartitions) {
        fromOffsets += ((TopicAndPartition(TOPIC_NAME, partition), 0))
      }
    } else { // 如果既没有新增加的分区，也不是第一次运行
      // 获取上次运行的offset
      for (partition <- 0 until hbaseNumberOfPartitions) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += ((TopicAndPartition(TOPIC_NAME, partition), fromOffset.toLong))
      }
    }

    scanner.close()
    conn.close()
    fromOffsets.toMap
  }

  def main(args: Array[String]): Unit = {
    val processingInterval = 2
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topics = "mytest1"
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("kafkahbase").setMaster("local[2]")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    val groupId = "testp"
    val hbaseTableName = "spark_kafka_offsets"

    // 获取kafkaStream
    //val kafkaStream = createMyDirectKafkaStream(ssc, kafkaParams, zkClient, topicsSet, "testp")
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    // 获取offset
    val fromOffsets = getLastestOffsets("mytest1", groupId, hbaseTableName, "node01:2181,node02:2181,node03:2181", "kafka0.9", 30000, 30000)

    var kafkaStream: InputDStream[(String, String)] = null
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    kafkaStream.foreachRDD((rdd, btime) => {
      if (!rdd.isEmpty()) {
        println("==========================:" + rdd.count())
        println("==========================btime:" + btime)
        saveOffsets(topics, groupId, rdd.asInstanceOf[HasOffsetRanges].offsetRanges, hbaseTableName, btime)
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}