package com.ibm.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
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
    private String data = RandomStringUtils.randomAlphabetic(1024);
    private final Document[] toRead = new Document[9];
    private int readIndex = 0;
    private long maxReadlatency = 0;
    private long minReadLatency = Long.MAX_VALUE;
    private long maxWriteLatency = 0;
    private long minWriteLatency = Long.MAX_VALUE;
    private float accReadLatencies = 0;
    private float accWriteLatencies = 0;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private final float targetRate;
    private long startMillis;
    private long elapsed = 0l;
    private FileOutputStream readLatencySink;
    private FileOutputStream insertLatencySink;
    private String lineSeparator = System.getProperty("line.separator");
    private String prefixLatencyFile;

    public RunThread(String host, List<Integer> ports, float targetRate, String prefixLatencyFile) {
        this.host = host;
        this.ports = ports;
        this.targetRate = targetRate;
        for (int i = 0; i < 9; i++) {
            toRead[i] = new Document("_id", i);
        }
        this.prefixLatencyFile = prefixLatencyFile;
    }

    @Override
    public void run() {
        int portsLen = ports.size();
        final MongoClient[] clients = new MongoClient[portsLen];
        log.info("Opening {} connections", portsLen);
        for (int i = 0; i < portsLen; i++) {
            final MongoClientOptions ops = MongoClientOptions.builder()
                    .maxWaitTime(120000)
                    .connectTimeout(120000)
                    .socketTimeout(120000)
                    .build();
            clients[i] = new MongoClient(new ServerAddress(host, ports.get(i)), ops);
        }

        if (prefixLatencyFile != null) {
            try {
                readLatencySink = new FileOutputStream(prefixLatencyFile + "_read_" + Thread.currentThread().getId());
                insertLatencySink = new FileOutputStream(prefixLatencyFile + "_insert_" + Thread.currentThread().getId());
            } catch (IOException e) {
                log.error("Unable to open latency file", e);
            }
        }

        int clientIdx = 0;
        initialized.set(true);
        long ratePause = (long) (1000f / targetRate);
        startMillis = System.currentTimeMillis();
        float currentRate = 0;

        // do the actual benchmark measurements
        try {
            while (!stop.get()) {
                currentRatio = (float) numReads / (float) (numInserts + numReads);
                if (targetRate > 0) {
                    if ((float) (numReads + numInserts) * 1000f / (float) (System.currentTimeMillis() - startMillis) > targetRate) {
                        sleep(ratePause);
                    }
                }
                clientIdx = clientIdx + 1 < clients.length ? clientIdx + 1 : 0;
                if (currentRatio < targetRatio) {
                    readRecord(clients[clientIdx]);
                } else {
                    insertRecord(clients[clientIdx]);
                }
                elapsed = System.currentTimeMillis() - startMillis;
            }
        } catch (IOException e) {
            log.error("Error while running benchmark", e);
        }

        log.info("Closing {} connections", clients.length);
        for (final MongoClient c : clients) {
            c.close();
        }

        try {
            if (insertLatencySink != null) {
                insertLatencySink.close();
            }
            if (readLatencySink != null) {
                readLatencySink.close();
            }
        } catch (IOException e) {
            log.error("Unable to close stream", e);
        }

        log.info("Thread finished");
    }

    private void sleep(long ratePause) {
        try {
            Thread.sleep(ratePause);
        } catch (InterruptedException e) {
            log.error("Error while sleeping", e);
        }
    }

    public float getRate() {
        return ((float) (numInserts + numReads) * 1000f) / (float) elapsed;
    }

    private void insertRecord(MongoClient client) throws IOException {
        long start = System.nanoTime();
        client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME).insertOne(new Document("data", data));
        long latency = System.nanoTime() - start;
        recordLatency(latency, insertLatencySink);
        if (latency < minWriteLatency) {
            minWriteLatency = latency;
        }
        if (latency > maxWriteLatency) {
            maxWriteLatency = latency;
        }
        accWriteLatencies += latency;
        numInserts++;
    }

    private void readRecord(MongoClient client) throws IOException {
        final Document doc = toRead[readIndex];
        long start = System.nanoTime();
        final Document fetched = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME).find(toRead[readIndex]).first();
        long latency = System.nanoTime() - start;
        recordLatency(latency, readLatencySink);
        if (latency < minReadLatency) {
            minReadLatency = latency;
        }
        if (latency > maxReadlatency) {
            maxReadlatency = latency;
        }
        accReadLatencies += latency;
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

    private void recordLatency(final long latency, final FileOutputStream sink) throws IOException {
        if (sink != null) {
            sink.write(String.valueOf(latency).getBytes());
            sink.write(lineSeparator.getBytes());
            sink.flush();
        }
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

    public long getMaxReadlatency() {
        return maxReadlatency;
    }

    public long getMaxWriteLatency() {
        return maxWriteLatency;
    }

    public long getMinReadLatency() {
        return minReadLatency;
    }

    public long getMinWriteLatency() {
        return minWriteLatency;
    }

    public float getAccReadLatencies() {
        return accReadLatencies;
    }

    public float getAccWriteLatencies() {
        return accWriteLatencies;
    }


    public boolean isInitialized() {
        return initialized.get();
    }

    public synchronized void resetData() {
        numInserts = 0;
        numReads = 0;
        readIndex = 0;
        maxReadlatency = 0;
        minReadLatency = Long.MAX_VALUE;
        maxWriteLatency = 0;
        minWriteLatency = Long.MAX_VALUE;
        accReadLatencies = 0;
        accWriteLatencies = 0;
        startMillis = System.currentTimeMillis();
    }
}
