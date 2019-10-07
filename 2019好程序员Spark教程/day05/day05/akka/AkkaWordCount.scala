package com.qf.gp1922.day05.akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.io.Source

class AkkaWordCount extends Actor{
  override def receive: Receive = {
    case SubmitTask(fileName) => {
      val content: String = Source.fromFile(fileName).mkString
      val arr: Array[String] = content.split("\r\n")
      val result: Map[String, Int] = arr.flatMap(_.split(" ")).groupBy(x => x).mapValues(_.size)
      sender ! ResultTask(result)
    }

    case StopTask => sys.exit()
  }
}
object AkkaWordCount {
  def main(args: Array[String]): Unit = {
    // 指定host和port
    val host = "127.0.0.1"
    val port = 6666

    // 配置信息
    val conf = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
      """.stripMargin
    )

    // 创建用于创建actor的对象
    val actorSystem: ActorSystem = ActorSystem.create("ServerSystem", conf)

    // 创建actor
    val server: ActorRef = actorSystem.actorOf(Props(new AkkaWordCount), "Server")

    // 指定要获取数据的file
    val file = "c://data/file.txt"

    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 seconds)

    import akka.pattern.ask
    val reply = server ? SubmitTask(file)
    // 设置等待返回值的超时时间
    val result = Await.result(reply, timeout.duration).asInstanceOf[ResultTask]

    println(result)

    server ! StopTask
  }
}

case class SubmitTask(fileName: String)
case class ResultTask(result: Map[String, Int])
case object StopTask