package com.qcs.IM;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.uring.IoUringChannelOption;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class MessageMain {
    public static ConcurrentHashMap<Integer, User> users = new ConcurrentHashMap<>();
    public static MessageProducerProc messageTailerProc = new MessageProducerProc();
    public static MessageConsumerProc messageAppenderProc = new MessageConsumerProc();
    public static ConcurrentHashMap<Integer, MessageRoom> messageRooms = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Integer, ChannelHandlerContext> onLineChannels = new ConcurrentHashMap<>();
    public static volatile long startTime = 0;
    public static volatile long endTime = 0;
    public static void main(String[] args) throws InterruptedException {
        messageTailerProc.start();
        messageAppenderProc.start();

        ConcurrentHashMap<Integer,Integer> friends = new ConcurrentHashMap<>();
        friends.put(1,1);
        friends.put(2,2);

        messageRooms.put(1, new MessageRoom(1,2));

        users.put(1, new User(1, friends, messageRooms));
        users.put(2, new User(2, friends, messageRooms));

        EventLoopGroup boosGroup = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory(IoUringConfig.CONFIG));
        EventLoopGroup workGroup = new MultiThreadIoEventLoopGroup(2, IoUringIoHandler.newFactory(IoUringConfig.CONFIG));
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workGroup);
        serverBootstrap.option(IoUringChannelOption.SO_REUSEPORT, true);

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true,1,1,8192,11,0,0,true);
        serverBootstrap.option(ChannelOption.ALLOCATOR, allocator);
        serverBootstrap.childOption(ChannelOption.ALLOCATOR, allocator);

        // ========== 子Channel参数优化 ==========
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        serverBootstrap.childOption(ChannelOption.SO_SNDBUF, 1024 * 1024 * 16);
        serverBootstrap.childOption(ChannelOption.SO_RCVBUF, 1024 * 1024);
        // 禁用自动读取 - 手动控制背压
        //serverBootstrap.childOption(ChannelOption.AUTO_READ, false);
        serverBootstrap.channel(IoUringServerSocketChannel.class)
                // 4. 设置连接队列大小
                .option(ChannelOption.SO_BACKLOG, 128)
                // 5. 保持连接活跃
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                // 6. 添加处理器到每个新连接的 Channel
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LineBasedFrameDecoder(256))
                                .addLast(new StringDecoder(StandardCharsets.UTF_8))   // ByteBuf -> String
                                .addLast(new StringEncoder(StandardCharsets.UTF_8))   // String -> ByteBuf
                                .addLast(new MessageHandler());
                    }
                });

        // 7. 绑定端口并启动
        serverBootstrap.bind(7090).sync();
    }
}
