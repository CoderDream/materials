package com.qf.gp1922.day14.kafkaapi

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

/**
  * 实现consumer
  */
object KafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val prop = new Properties
    prop.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    // 指定消费者组
    prop.put("group.id", "group01")
    // 指定消费位置: earliest/latest/none
    prop.put("auto.offset.reset", "earliest")
    // 指定消费的key的反序列化方式
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列化方式
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    // 得到Consumer实例
    val consumer = new KafkaConsumer[String, String](prop)

    // 首先需要订阅topic
    consumer.subscribe(Collections.singletonList("test"))
    // 开始消费数据
    while (true) {
      val msgs: ConsumerRecords[String, String] = consumer.poll(1000)
      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: ${msg.key()}, value: ${msg.value()}")
      }
    }


  }
}
