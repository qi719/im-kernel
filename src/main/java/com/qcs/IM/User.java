package com.qcs.IM;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class User {
    private int id;
    private AtomicBoolean online;
    private ConcurrentHashMap<Integer,Integer> friends;
    private ConcurrentHashMap<Integer, MessageRoom> rooms;
    private ConcurrentHashMap<Integer, ExcerptTailer> tailers;
    private ConcurrentHashMap<Integer, ExcerptAppender> appenders;

    public User(int userID, ConcurrentHashMap<Integer,Integer> friends, ConcurrentHashMap<Integer, MessageRoom> rooms){
        this.id = userID;
        this.friends = friends;
        this.rooms = rooms;
        online = new AtomicBoolean(false);
        tailers = new ConcurrentHashMap<>();
        appenders = new ConcurrentHashMap<>();
        getTailer();
        getAppender();
    }

    public void login(){
        online.set(true);
    }

    public boolean loggedIn() {
        return online.get();
    }

    public void sendMessage(int roomId, String message) {
        try(DocumentContext documentContext = appenders.get(roomId).writingDocument()){
            Bytes<?> bytes = documentContext.wire().bytes();
            bytes.writeInt(this.id);
            bytes.writeUtf8(message);
        };
    }

    public void getTailer() {
        for (Integer i : rooms.keySet()) {
            ExcerptTailer tailer = rooms.get(i).getTailer(this.id);
            tailers.put(i, tailer);
        }
    }

    public ConcurrentHashMap<Integer, ExcerptTailer> getUserTailer() {
        return tailers;
    }

    public void getAppender() {
        for (Integer i : rooms.keySet()) {
            ExcerptAppender tailer = rooms.get(i).getAppender();
            appenders.put(i, tailer);
        }
    }

    public void addFriend(int id) {
        this.friends.put(id,id);
        createRoom(id, this.id);
        tailers.put(id, rooms.get(id).getTailer(this.id));
        appenders.put(id, rooms.get(id).getAppender());
    }

    public int getId(){
        return this.id;
    }

    private void createRoom(Integer... ids) {
        MessageRoom messageRoom = new MessageRoom(ids);
        rooms.put(ids[0], messageRoom);
    }
}
