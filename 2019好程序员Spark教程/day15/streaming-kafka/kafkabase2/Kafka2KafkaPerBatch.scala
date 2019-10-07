package com.qf.kafkatest.kafkabase2

import java.util
import java.util.Properties

import com.qf.kafkatest.kafkabase2.utils.MyKafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * 将kafka中的数据消费后写入到kafka， 按照batch的方式。
  * 使用广播变量 将kafka创建生产者广播到每个executor上面
  */
object Kafka2KafkaPerBatch {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val processingInterval = 2
    val brokers = "node01:9092,node02:9092,node03:9092"
    val topic = "mytest1"
    val topicsSet = topic.split(",").toSet
    val groupName = "group02"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sc, Seconds(processingInterval))

    val streaming = MyKafkaUtils.createMyDirectKafkaStream(
      ssc, kafkaParams, Set(topic), groupName)

    val sinkTopic = "mykafka"

    // Kafka的Producer不能序列化
    // Caused by: java.io.NotSerializableException: org.apache.kafka.clients.producer.KafkaProducer
    //    streaming.foreachRDD(rdd=>{
    //      if(!rdd.isEmpty()){
    //        val props = new util.HashMap[String, Object]()
    //        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    //        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    //          "org.apache.kafka.common.serialization.StringSerializer")
    //        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    //          "org.apache.kafka.common.serialization.StringSerializer")
    //        val producer = new KafkaProducer[String,String](props)
    //
    //        rdd.map(x=>x._2).map(msg=>ParseUtils.parseMsg(msg)).foreach(msg=>{
    //
    //          val message=new ProducerRecord[String, String]( sinkTopic ,null,msg)
    //          producer.send(message)
    //        })
    //      }
    //    })


    // 数据可以写入到kafka， 但是性能差， 每条记录都需要创建producer
    // streaming.foreachRDD(rdd=>{
    //      if(!rdd.isEmpty()){
    //        rdd.map(x=>x._2).map(msg=>ParseUtils.parseMsg(msg)).filter(_.length!=1).foreach(msg=>{
    //
    //          val props = new util.HashMap[String, Object]()
    //          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    //          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    //            "org.apache.kafka.common.serialization.StringSerializer")
    //          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    //            "org.apache.kafka.common.serialization.StringSerializer")
    //          val producer = new KafkaProducer[String,String](props)
    //          val message=new ProducerRecord[String, String]( sinkTopic ,null,msg)
    //          producer.send(message)
    //        })
    //      }
    //    })

    // 推荐：
    // 将KafkaProducer对象广播到所有的executor节点，
    // 这样就可以在每个executor节点将数据插入到kafka
//    val kafkaProducer: Broadcast[MyKafkaProducer[String, String]] = {
//      val kafkaProducerConfig = {
//        val p = new Properties()
//        p.setProperty("bootstrap.servers", brokers)
//        p.setProperty("key.serializer", classOf[StringSerializer].getName)
//        p.setProperty("value.serializer", classOf[StringSerializer].getName)
//        p
//      }
//      ssc.sparkContext.broadcast(MyKafkaProducer[String, String](kafkaProducerConfig))
//    }
//
//    streaming.foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        rdd.map(x => x._2).map(msg => ParseUtils.parseMsg(msg)).filter(_.length != 1).foreach(msg => {
//          kafkaProducer.value.send(sinkTopic, msg)
//        })
//        MyKafkaUtils.saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
//      }
//    })

    // 推荐：
    // 用partition的方式，一个rdd的partition对应一个KafkaProducer
    streaming.foreachRDD(rdd=>rdd.foreachPartition(
      // 该rdd的partition对应着kafka里topic的partition
      partition=>{
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        // 创建的producer在partition里而不是在foreach里，这样减少了KafkaProducer对象的个数
        val producer = new KafkaProducer[String,String](props)

        partition.map(msg=>ParseUtils.parseMsg(msg._2)).filter(_.length!=1).foreach(msg=>{
          val message=new ProducerRecord[String, String](sinkTopic, null, msg)
          producer.send(message)
        })

        MyKafkaUtils.saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
      }
    ))



    ssc.start()
    ssc.awaitTermination()
  }

}
