package com.qcs.IM;

import io.netty.channel.*;

import static com.qcs.IM.MessageMain.messageTailerProc;

@ChannelHandler.Sharable
public class MessageHandler extends SimpleChannelInboundHandler<String> {
    //private static final Pattern PATTERN = Pattern.compile("\\|");
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, String s) throws Exception {
        //System.out.println(s);
        //if (PATTERN.matcher(s).matches()) {
        String[] split = s.split("\\|");
        //userId|type|roomId|content
        messageTailerProc.addMsg(split, channelHandlerContext);
        //}
        //channelHandlerContext.writeAndFlush("ok");
    }
}
