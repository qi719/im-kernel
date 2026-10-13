package com.qcs.IM;

import org.jctools.queues.MpscUnboundedXaddArrayQueue;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Test {
    static ExecutorService fixedThreadPool = Executors.newFixedThreadPool(4);
    static MpscUnboundedXaddArrayQueue eventQueue = new MpscUnboundedXaddArrayQueue(1024);
    public static void main(String[] args) {
        MessageTailerProc messageTailerProc = new MessageTailerProc("ThreadPool");
        for (int i = 0; i < 100000; i++) {
            fixedThreadPool.submit(messageTailerProc);
        }

        Thread t = new Thread(new MessageMpscProc("Mpsc"));
        t.start();
        for (int i = 0; i < 100000; i++) {
            eventQueue.add(1);
        }

    }

    public static class MessageTailerProc implements Runnable{
        private AtomicLong startTime;
        private AtomicLong endTime;
        private AtomicInteger counter = new AtomicInteger(0);
        private String name = "";
        public MessageTailerProc(String name){
            startTime = new AtomicLong(System.nanoTime());
            this.name = name;
        }
        @Override
        public void run() {
            counter.incrementAndGet();
            if(counter.get() == 9999){
                endTime = new AtomicLong(System.nanoTime());
                long durationMillis = (endTime.get() - startTime.get())/1_000_000;
                System.out.println(this.name+"执行耗时: " + durationMillis + " 毫秒");
            }
        }
    }
    public static class MessageMpscProc implements Runnable{
        private volatile long startTime;
        private volatile long endTime;
        private volatile long counter = 0;
        private String name = "";
        public MessageMpscProc(String name){
            startTime = System.nanoTime();
            this.name = name;
        }
        @Override
        public void run() {
            while ( true) {
                Integer poll = (Integer) eventQueue.poll();
                if(poll != null) {
                    counter++;
                    if (counter == 9999) {
                        endTime = System.nanoTime();
                        long durationMillis = (endTime - startTime) / 1_000_000;
                        System.out.println(this.name + "执行耗时: " + durationMillis + " 毫秒");
                    }
                }
                Thread.yield();
            }
        }
    }
}
