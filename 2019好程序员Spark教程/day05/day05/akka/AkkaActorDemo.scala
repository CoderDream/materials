package com.qf.gp1922.day05.akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class AkkaActorDemo extends Actor{
  override def receive: Receive = {
    case "start" => {
      println("starting")
      Thread.sleep(3000)
      println("started")
    }
    case "stop" => {
      println("stopping...")
      Thread.sleep(2000)
      println("stopped")
    }
  }
}

object AkkaActorDemo {
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
    val server: ActorRef = actorSystem.actorOf(Props[AkkaActorDemo], "Server")

    // 向actor发送消息,
    // 有两个方法，其中“！”代表异步发送消息没有返回值，
    // 还有一个“？”代表同步发送消息并线程等待返回值
    server ! "start"
    server ! "stop"
    println("消息发送完成")
  }
}
