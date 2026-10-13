package com.qcs.IM;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringAdaptiveBufferRingAllocator;
import io.netty.channel.uring.IoUringBufferRingConfig;
import io.netty.channel.uring.IoUringIoHandlerConfig;

public class IoUringConfig {
    public static IoUringIoHandlerConfig CONFIG = GetIoUringConfig();
    public static IoUringIoHandlerConfig GetIoUringConfig() {
        IoUringIoHandlerConfig config = new IoUringIoHandlerConfig();
        IoUringBufferRingConfig bufferRingConfig = IoUringBufferRingConfig.builder()
                .bufferGroupId((short) 0)
                .bufferRingSize((short) 8192)
                .batchSize(4096)
                .batchAllocation(true)
                .allocator(new IoUringAdaptiveBufferRingAllocator(
                        PooledByteBufAllocator.DEFAULT,
                        512,
                        4096,
                        16384,
                        true
                )).build();
        config.setBufferRingConfig(bufferRingConfig);
        int ringSize = config.getRingSize();
        config.setRingSize(ringSize * 4);
        if(IoUring.isAcceptMultishotEnabled()){
            System.out.println("accept multishot enabled");
        }
        return config;
    }
}
