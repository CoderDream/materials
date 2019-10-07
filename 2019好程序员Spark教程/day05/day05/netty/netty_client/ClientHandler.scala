package com.qf.gp1922.day05.netty.netty_client

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

class ClientHandler extends ChannelInboundHandlerAdapter{
  /**
    * 请求服务端并发送消息
    * @param ctx
    */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("发送消息的请求")
    val content = "Hello How are you"
    ctx.writeAndFlush(Unpooled.copiedBuffer(content.getBytes("UTF-8")))
  }

  /**
    * 用于接收服务端发送过来的消息
    * @param ctx
    * @param msg
    */
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    println("接收到服务端发送过来的消息")
    // 接收数据
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message: String = new String(bytes, "UTF-8")
    println(message)
  }

  /**
    * 将消息队列汇总的数据写入到SocketChannel并发送
    * @param ctx
    */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }
}
