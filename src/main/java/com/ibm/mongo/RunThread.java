package com.ibm.mongo;

import com.mongodb.MongoClient;
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RunThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RunThread.class);
    private AtomicBoolean stop = new AtomicBoolean(false);
    private float targetRatio = 0.9f;
    private float currentRatio = 0f;
    private int numInserts = 0;
    private int numReads = 0;
    private final String host;
    private final List<Integer> ports;
    private long duration;
    private String data = RandomStringUtils.randomAlphabetic(1024);
    private final Document[] toRead = new Document[9];
    private int readIndex = 0;

    public RunThread(String host, List<Integer> ports) {
        this.host = host;
        this.ports = ports;
        for (int i = 0; i < 9;i++) {
            toRead[i] = new Document("_id", i);
        }
    }

    @Override
    public void run() {
        int portsLen = ports.size();
        final MongoClient[] clients = new MongoClient[portsLen];
        log.info("Opening {} connections", portsLen);
        for (int i = 0; i< portsLen;i++) {
            clients[i] = new MongoClient(host, ports.get(i));
        }

        int clientIdx = 0;
        long start = System.currentTimeMillis();
        while (!stop.get()) {
            currentRatio = (float) numReads / (float) (numInserts + numReads);
            clientIdx = clientIdx + 1 < clients.length ? clientIdx + 1 : 0;
            if (currentRatio < targetRatio) {
                readRecord(clients[clientIdx]);
            } else {
                insertRecord(clients[clientIdx]);
            }
        }
        duration = System.currentTimeMillis() - start;
        log.info("Closing {} connections", clients.length);
        for (final MongoClient c: clients) {
            c.close();
        }

        log.info("Thread finished");
    }

    public float getRate() {
        return ((float) (numInserts + numReads) * 1000f) / (float) duration;
    }

    private void insertRecord(MongoClient client) {
        client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME).insertOne(new Document("data", data));
        numInserts++;
    }

    private void readRecord(MongoClient client) {
        final Document doc = toRead[readIndex];
        final Document fetched = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME).find(toRead[readIndex]).first();
        if (fetched == null) {
            log.warn("Unable to read document with id {}", doc.get("_id"));
        }
        if (readIndex + 2 >= toRead.length) {
            readIndex = 0;
        } else {
            readIndex++;
        }
        numReads++;
    }

    public void stop() {
        stop.set(true);
    }

    public int getNumInserts() {
        return numInserts;
    }

    public int getNumReads() {
        return numReads;
    }
}
