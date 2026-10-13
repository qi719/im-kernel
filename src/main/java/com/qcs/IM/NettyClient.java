package com.qcs.IM;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class NettyClient {
    public static void main(String[] args) throws InterruptedException {
        String host = "192.168.57.128";
        int port = 7090;
        //String message = "Hello Netty";

        // 1. 创建事件循环组（处理网络事件）
        EventLoopGroup group = new NioEventLoopGroup();
        PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        ByteBuf buffer = allocator.buffer();
        buffer.writeByte(49);
        System.out.println(ByteBuffer.wrap(buffer.array()).array());
        try {
            // 2. 创建客户端启动器
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)                          // 设置线程组
                    .channel(NioSocketChannel.class)        // 设置NIO通道类型
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new LineBasedFrameDecoder(256))
                                    // String解码：ByteBuf -> String
                                    .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                    // String编码：String -> ByteBuf
                                    .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                    .addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    // 3. 连接建立时发送消息
                                    ctx.writeAndFlush("1|0\n");
                                    long startTime = System.nanoTime();
                                    for (int i = 0; i < 9999; i++) {
                                        ctx.write("1|1|阿萨德将复何及阿斯蒂芬户籍卡三大步伐卡手机大发不卡登记上吧发卡机啊巴萨"+i+"\n");
                                    }
                                    ctx.flush();
                                    long endTime = System.nanoTime();
                                    long durationNanos = endTime - startTime;
                                    double durationMillis = durationNanos / 1_000_000.0;

                                    System.out.printf("执行耗时: %d 纳秒 (约 %.2f 毫秒)%n", durationNanos, durationMillis);
                                    System.out.println("已发送: ");
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                    // 4. 接收响应（如果需要）
                                    System.out.println("收到响应: " + msg);
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            // 5. 连接到服务器并等待连接完成
            ChannelFuture future = bootstrap.connect(host, port).sync();

            // 6. 等待连接关闭（发送完成后服务器会关闭连接）
            future.channel().closeFuture().sync();
        } finally {
            // 7. 优雅退出，释放线程资源
            group.shutdownGracefully();
        }
    }
}
