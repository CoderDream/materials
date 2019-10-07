package com.qf.gp1922.day15

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 实时消费kafka的topic的数据进行分析
  */
object LoadKafkaDataDemo {
  def main(args: Array[String]): Unit = {
    val checkpointDir = "d://cp-20190810-2"
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => createContext)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 该方法包含主要计算逻辑，返回StreamingContext
    */
  def createContext = {
    // 创建上下文
    val conf = new SparkConf().setAppName("LoadKafkaDataDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    ssc.checkpoint("d://out-20190810-3")

    // 指定请求kafka的配置信息
    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      // 指定key的反序列化方式
      "key.deserializer" -> classOf[StringDeserializer],
      // 指定value的反序列化方式
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // 指定消费位置
      "auto.offset.reset" -> "latest",
      // 如果value合法，自动提交offset
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("test1")

    // 消费数据
    val logs: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
      ssc,
        /**
          * 消费数据的本地策略：
          * 1. LocationStrategies.PreferBrokers()
          * 仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在  kafka broker 的机器上；
          * 2. LocationStrategies.PreferConsistent();
          * 大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
          * 3. LocationStrategies.PreferFixed(hostMap: collection.Map[TopicPartition, String])
          * 4. LocationStrategies.PreferFixed(hostMap: ju.Map[TopicPartition, String])
          * 如果你的负载不均衡，可以通过这两种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配；
          */
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParam)
    )

//    // 开始操作数据并观察数据结构
//    logs.foreachRDD(rdd => {
//      // 调用offsetRanges方法获取数据对应的offset的范围
//      val offsetList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreachPartition(part => {
//        part.foreach(line => {
//          val offsetInfo: OffsetRange = offsetList(TaskContext.get().partitionId())
//          println("topicPartition:" + offsetInfo.topicPartition())
//          println("topic:" + offsetInfo.topic)
//          println("fromOffset:" + offsetInfo.fromOffset)
//          println("line:" + line)
//        })
//      })
//    })

    // 对消费的数据做单词计数
    // 其中key的数据不需要，仅仅留下value，因为value是实际的log日志数据
    val lines: DStream[String] = logs.map(_.value())
    val tups: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val res = tups.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    res.print

    ssc
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map{
      case (a,b,c) => {
        (a, b.sum + c.getOrElse(0))
      }
    }
  }
}
