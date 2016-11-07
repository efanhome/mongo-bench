package com.ibm.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

public class LoadThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoadThread.class);

    private final String host;
    private final List<Integer> ports;
    private final int numDocuments;
    private final int docSize;
    private final int maxBatchSize = 1000;
    private final int timeoutMs;

    private final static DecimalFormat decimalFormat = new DecimalFormat("0.0000");

    public LoadThread(String host, List<Integer> ports, int numDocuments, int docSize, int timeout) {
        this.host = host;
        this.ports = ports;
        this.numDocuments = numDocuments;
        this.docSize = docSize;
        this.timeoutMs = timeout * 1000;
    }

    @Override
    public void run() {
        log.info("Loading data into {} instances on {}", ports.size(), host);
        for (int i = 0; i < ports.size(); i++) {
            int count = 0, currentBatchSize;
            final MongoClientOptions ops = MongoClientOptions.builder()
                    .maxWaitTime(timeoutMs)
                    .connectTimeout(timeoutMs)
                    .socketTimeout(timeoutMs)
                    .heartbeatConnectTimeout(timeoutMs)
                    .serverSelectionTimeout(timeoutMs)
                    .build();
            final MongoClient client = new MongoClient(new ServerAddress(host, ports.get(i)), ops);
            for (final String name : client.listDatabaseNames()) {
                if (name.equalsIgnoreCase(MongoBench.DB_NAME)) {
                    log.warn("Database {} exists and will be purged before inserting", MongoBench.DB_NAME);
                    client.dropDatabase(name);
                    break;
                }
            }
            long startLoad = System.currentTimeMillis();
            while (count < numDocuments) {
                currentBatchSize = numDocuments - count > maxBatchSize ? maxBatchSize : numDocuments - count;
                final Document[] docs = createDocuments(currentBatchSize, count);
                final MongoCollection<Document> collection = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
                collection.insertMany(Arrays.asList(docs));
                count += currentBatchSize;
            }
            client.close();
            long duration = System.currentTimeMillis() - startLoad;
            float rate = 1000f * 1000f / (float) duration;
            log.info("Finished loading {} documents in {}:{} [{} inserts/sec]", count, host, ports.get(i), rate);
        }


//        final Document[] docs = createDocuments();
//        for (int i = 0; i < ports.size(); i++) {
//            long startLoad = System.currentTimeMillis();
//            final MongoClient client = new MongoClient(host, ports.get(i));
//            for (String name : client.listDatabaseNames()) {
//                if (name.equalsIgnoreCase(MongoBench.DB_NAME)) {
//                    log.warn("Database {} exists and will be purged before inserting", MongoBench.DB_NAME);
//                    client.dropDatabase(MongoBench.DB_NAME);
//                    break;
//                }
//            }
//            final MongoCollection<Document> collection = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
//            collection.insertMany(Arrays.asList(docs));
//            client.close();
//            long duration = System.currentTimeMillis() - startLoad;
//            float rate = 1000f * 1000f / (float) duration;
//            log.info("Finished loading {} documents in MongoDB on {}:{} in {} ms {} inserts/sec", docs.length, host, ports.get(i), duration, decimalFormat.format(rate));
//        }
//        log.info("Load phase finished");
    }

    private Document[] createDocuments(int count, int offset) {
        final String data = RandomStringUtils.randomAlphabetic(docSize);
        final Document[] docs = new Document[count];
        for (int i = 0; i < count; i++) {
            docs[i] = new Document()
                    .append("_id", i + offset)
                    .append("data", data);
        }
        return docs;
    }
}
