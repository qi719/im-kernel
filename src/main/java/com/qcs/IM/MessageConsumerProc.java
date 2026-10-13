package com.qcs.IM;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.concurrent.ConcurrentHashMap;

import static com.qcs.IM.MessageMain.*;
import static com.qcs.IM.MessageProducerProc.counter;

public class MessageConsumerProc extends Thread {
    public static volatile boolean max = false;
    @Override
    public void run() {
        while (true) {
            if(counter == 9999 && !max){
                max = true;
                endTime = System.nanoTime();
                long durationNanos = endTime - startTime;
                double durationMillis = durationNanos / 1_000_000.0;

                System.out.printf("执行耗时: %d 纳秒 (约 %.2f 毫秒)%n", durationNanos, durationMillis);
            }
            for (Integer i : onLineChannels.keySet()) {
                User user = users.get(i);
                if(user.loggedIn()){
                    ConcurrentHashMap<Integer, ExcerptTailer> userTailer = user.getUserTailer();
                    for (Integer j : userTailer.keySet()){
                        DocumentContext documentContext = userTailer.get(j).readingDocument();
                        if (!documentContext.isPresent()) {
                            continue;
                        }
                        Bytes<?> bytes = documentContext.wire().bytes();
                        int i1 = bytes.readInt();

                        if (user.getId() != i1) {
                            String b = bytes.readUtf8();
                            onLineChannels.get(i).writeAndFlush(b+"\r\n");
                            counter++;
                        }
                        documentContext.close();

                    }
                }
            }
            Thread.yield();

        }
    }
}
