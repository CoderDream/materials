package com.qf.kafkatest.kafkabase1

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.util.Random

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {
  def main(args: Array[String]) {
    // metadataBrokerList：kafka列表，topic：topic名称，
    // messagesPerSec：每秒的消息数，wordsPerMessage：每秒的单词数量
    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic>")
      // 退出程序
      // system.exit(0):正常退出，相当于shell的kill
      // system.exit(1):非正常退出，相当于shell的kill -9
      System.exit(1)
    }
    // args: node01:9092,node02:9092,node03:9092 kefkawc
    val Array(brokers, topic) = args
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val arr = Array(
      "hello tom",
      "hello jerry",
      "hello kitty",
      "hello suke"
    )
    val r = new Random();
    // Send some messages
    while (true) {
      val message = arr(r.nextInt(arr.length))
      producer.send(new ProducerRecord[String, String]("kafkawc", message))
      Thread.sleep(1000)
    }
  }

}
