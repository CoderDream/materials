package com.qf.gp1922.day05.netty.netty_server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

class NettyServer {
  def bind(host: String, port: Int): Unit ={
    // 配置服务的线程组,用于服务接收client端的连接
    val group = new NioEventLoopGroup()
    // 用户进行网络的读写
    val loopGroup = new NioEventLoopGroup()
    // 启动NIO服务端的辅助类
    val bootstrap = new ServerBootstrap()
    bootstrap.group(group, loopGroup).channel(classOf[NioServerSocketChannel])

    // 绑定IO事件
    bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(c: SocketChannel) = {
        c.pipeline().addLast(new ServerHandler)
      }
    })

    // 绑定端口
    val channelFuture = bootstrap.bind(host, port).sync()

    // 等待服务关闭
    channelFuture.channel().closeFuture().sync()

    // 释放资源
    group.shutdownGracefully()
    loopGroup.shutdownGracefully()
  }
}
object NettyServer {
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1" // localhost
    val port = 8888
    val server = new NettyServer
    server.bind(host, port)
  }
}