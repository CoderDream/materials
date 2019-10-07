package com.qf.gp1922.day05.netty.netty_client

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

class NettyClient {
    def bind(host: String, port: Int): Unit ={
      // 配置服务的线程组,用于服务接收client端的连接
      val group = new NioEventLoopGroup()

      // 启动NIO服务端的辅助类
      val bootstrap = new Bootstrap
      bootstrap.group(group).channel(classOf[NioSocketChannel])

      // 绑定IO事件
      bootstrap.handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(c: SocketChannel) = {
          c.pipeline().addLast(new ClientHandler)
        }
      })

      // 绑定端口
      val channelFuture = bootstrap.connect(host, port).sync()

      // 等待服务关闭
      channelFuture.channel().closeFuture().sync()

      // 释放资源
      group.shutdownGracefully()
    }
}

object NettyClient {
  def main(args: Array[String]): Unit = {
    // 用于请求对方服务的host和port
    val serveHost = "127.0.0.1" // localhost
    val serverPort = 8888

    val client = new NettyClient
    client.bind(serveHost, serverPort)
  }
}
