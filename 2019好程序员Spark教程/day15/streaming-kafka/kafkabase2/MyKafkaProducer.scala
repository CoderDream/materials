package com.qf.kafkatest.kafkabase2

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import scala.collection.JavaConversions._

/**
  * 如果class和object同名， 那么称object为class的伴生对象。
  * 可以类比java的static修饰符。 因为scala中是没有static修饰符，
  * 那么object下的方法和成员变量等都是静态， 可以直接调用，而不需要创建对象。
  * 简单的说：对于class和object下面都有apply方法， 如果调用object下的Apply，使用类名()即可。
  * 如果调用class下的apply， 那么需要先创建一个类的对象， 然后使用对象名()调用， 如val ab = new MyKafkaProducer()，
  * 然后ab()就是调用class下的apply方法。
  */
class MyKafkaProducer[K,V](createProducer:()=>KafkaProducer[K, V]) extends Serializable{
  // KafkaProducer的lazy加载
  lazy val producer:KafkaProducer[K, V] = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

  def apply() = {}
}

object MyKafkaProducer{
  def apply[K, V](properties: java.util.Properties): MyKafkaProducer[K, V] = {
    val config = properties.toMap
    val createProducer = () => {
      val producer = new KafkaProducer[K, V](config)
	  // 确保在Producer关闭前，将缓冲的数据写入Kafka
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new MyKafkaProducer(createProducer)
  }
}

