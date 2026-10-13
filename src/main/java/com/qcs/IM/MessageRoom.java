package com.qcs.IM;

import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.WireType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MessageRoom {
    private ChronicleQueue queue;
    private List<Integer> userIds;
    private static final MessageConsumerProc MESSAGE_PROC = new MessageConsumerProc();
    private AtomicLong roomId;
    public MessageRoom(Integer... ids){
        userIds = new ArrayList<>();
        for (Integer id : ids) {
            userIds.add(id);
        }

        queue = ChronicleQueue.singleBuilder("/home/qcs/message/")
                .bufferCapacity(32<<20)
                .syncMode(SyncMode.ASYNC)
                .rollCycle(RollCycles.FAST_DAILY)
                .epoch(8 * 60 * 60 * 1000)
                .doubleBuffer(true)
                .blockSize(64<<20)
                .wireType(WireType.BINARY_LIGHT).storeFileListener(new StoreFileListener() {
                    @Override
                    public void onReleased(int cycle, File file) {
                        System.out.println("onReleased");
                    }

                    @Override
                    public void onAcquired(int cycle, File file) {
                        System.out.println("onAcquired");
                    }
                }).build();
        roomId = new AtomicLong(System.nanoTime());
    }

    public long getRoomId(){
        return roomId.get();
    }

    public List<Integer> getUserIds(){
        return userIds;
    }

    public ExcerptAppender getAppender(){
        return queue.createAppender();
    }

    public ExcerptTailer getTailer(int userId) {
        return queue.createTailer(String.valueOf(userId));
    }
}
