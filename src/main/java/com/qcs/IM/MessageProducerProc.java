package com.qcs.IM;

import io.netty.channel.ChannelHandlerContext;
import org.jctools.queues.MpscUnboundedXaddArrayQueue;

import static com.qcs.IM.MessageMain.onLineChannels;
import static com.qcs.IM.MessageMain.users;
import static com.qcs.IM.MessageMain.*;

public class MessageProducerProc extends Thread{
    public static volatile int counter = 0;
    private MpscUnboundedXaddArrayQueue eventQueue;
    public MessageProducerProc() {
        this.eventQueue = new MpscUnboundedXaddArrayQueue(1024);
    }

    public void addMsg(Object[]  msg, ChannelHandlerContext ctx){

        eventQueue.add(new Object[]{msg,  ctx});
    }

    public void addUser(int userId) {
        User user = users.get(userId);
        user.getTailer();
    }

    @Override
    public void run() {
        while (true) {
            Object[] poll = (Object[]) eventQueue.poll();
            if(poll == null) {
                continue;
            }
            if(counter == 0){
                startTime = System.nanoTime();
            }

            //String o = poll[0];
            String[] messageArr = (String[]) poll[0];
            String userId = messageArr[0];
            ChannelHandlerContext ctx = (ChannelHandlerContext) poll[1];
            switch (messageArr[1]) {
                case "0":
                    onLineChannels.put(Integer.parseInt(userId), ctx);
                    users.get(Integer.parseInt(userId)).login();
                    break;
                case "1":
                    users.get(Integer.parseInt(userId)).sendMessage(Integer.parseInt(messageArr[1]), messageArr[2]);
                    break;
                default:
                    break;
            }
            Thread.yield();

        }
    }
}
