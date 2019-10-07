package com.qf.gp1922.day05.netty.netty_server

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
  * 建立和客户端连接的handler
  */
class ServerHandler extends ChannelInboundHandlerAdapter{
  /**
    * 当客户端连接server后执行该方法
    * @param ctx
    */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("连接成功")
    Thread.sleep(2000)
  }

  /**
    * 用于接收客户端发送过来的消息
    * @param ctx
    * @param msg
    */
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    println("接收到客户端发送过来的消息")

    // 获取发送过来的数据
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message: String = new String(bytes, "UTF-8")
    println(message)

    // 响应信息
    val back = "再见客户端"
    val resp = Unpooled.copiedBuffer(back.getBytes("UTF-8"))
    ctx.write(resp)
  }

  /**
    * 将消息队列汇总的数据写入到SocketChannel并发送出去
    * @param ctx
    */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }
}
