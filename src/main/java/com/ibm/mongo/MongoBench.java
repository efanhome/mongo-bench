package com.ibm.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.commons.cli.*;
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;

public class MongoBench {

    private static final Logger log = LoggerFactory.getLogger(MongoBench.class);

    public final static String DB_NAME = "mongo-bench";

    public final static String COLLECTION_NAME = "mongo-bench-documents";

    private final static DecimalFormat decimalFormat = new DecimalFormat("#.00");

    private enum Phase {
        RUN, LOAD
    }

    public static void main(String[] args) {
        final Options ops = new Options();
        ops.addOption("p", "port", true, "The ports to connect to");
        ops.addOption("t", "target ", true, "The target host to connect to");
        ops.addOption("l", "phase", true, "The phase to execute [run|load]");
        ops.addOption("d", "duration", true, "Run the bench for this many seconds");
        ops.addOption("n", "num-thread", true, "The number of threads to run");
        ops.addOption("h", "help", false, "Show this help dialog");

        final CommandLineParser parser = new DefaultParser();
        final Phase phase;
        final int[] ports;
        final String host;
        int duration;
        int numThreads;

        try {
            final CommandLine cli = parser.parse(ops, args);
            if (cli.hasOption('h')) {
                showHelp(ops);
                return;
            }
            if (cli.hasOption('l')) {
                if (cli.getOptionValue('l').equalsIgnoreCase("load")) {
                    phase = Phase.LOAD;
                } else if (cli.getOptionValue('l').equalsIgnoreCase("run")) {
                    phase = Phase.RUN;
                } else {
                    throw new ParseException("Invalid phase " + cli.getOptionValue('l'));
                }
            } else {
                throw new ParseException("No phase given");
            }
            if (cli.hasOption('p')) {
                final String portVal = cli.getOptionValue('p');
                int dashIdx = portVal.indexOf('-');
                if (dashIdx == -1) {
                    ports = new int[1];
                    ports[0] = Integer.parseInt(portVal);
                } else {
                    int startPort = Integer.parseInt(portVal.substring(0, dashIdx));
                    int endPort = Integer.parseInt(portVal.substring(dashIdx + 1));
                    ports = new int[endPort - startPort + 1];
                    for (int i = 0; i <= endPort - startPort; i++) {
                        ports[i] = startPort + i;
                    }
                }
            } else {
                ports = new int[]{27017};
            }
            if (cli.hasOption('t')) {
                host = cli.getOptionValue('t');
            } else {
                host = "localhost";
            }
            if (cli.hasOption('d')) {
                duration = Integer.parseInt(cli.getOptionValue('d'));
            } else {
                duration = 60;
            }
            if (cli.hasOption('n')) {
                numThreads = Integer.parseInt(cli.getOptionValue('n'));
            } else {
                numThreads = 1;
            }

            log.info("Running phase {} against host {} on {} ports for {} seconds", phase.name(), host, ports.length, duration);

            final MongoBench bench = new MongoBench();
            if (phase == Phase.LOAD) {
                bench.doLoadPhase(host, ports);
            } else {
                if (numThreads > ports.length) {
                    throw new ParseException("Number of threads must be smaller than number of ports");
                }
                bench.doRunPhase(host, ports, duration, numThreads);
            }
        } catch (ParseException e) {
            log.error("Unable to parse", e);
        }
    }

    private void doRunPhase(String host, int[] ports, int duration, int numThreads) {
        log.info("Starting {} threads for {} instances", numThreads, ports.length);
        long start = System.currentTimeMillis();
        final Map<RunThread, Thread> threads = new HashMap<RunThread, Thread>(numThreads);
        final List<List<Integer>> slices = createSlices(ports, numThreads);
        for (int i = 0; i < numThreads; i++) {
            RunThread t = new RunThread(host,slices.get(i));
            threads.put(t, new Thread(t));
        }
        for (final Thread t : threads.values()) {
            t.start();
        }
        while (System.currentTimeMillis() - start < 1000 * duration) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("Unable to sleep", e);
            }
        }
        for (RunThread r : threads.keySet())  {
            r.stop();
        }
        long elapsed = System.currentTimeMillis() - start;
        for (Thread t : threads.values()) {
            try {
                t.join();
            } catch (InterruptedException e) {
                log.error("Unable to join thread", e);
            }
        }


        float avgRatePerThread = 0f;
        long numReads = 0;
        long numInserts = 0;
        for (final RunThread r : threads.keySet()) {
            avgRatePerThread += r.getRate();
            numInserts += r.getNumInserts();
            numReads += r.getNumReads();
        }
        float rate = (float) (numReads + numInserts) * 1000f / (float) elapsed;
        avgRatePerThread = avgRatePerThread / (float) numThreads;
        log.info("Read {} and inserted {} documents in {} secs", numReads, numInserts, decimalFormat.format((float) elapsed / 1000f));
        log.info("Overall transaction rate: {} transactions/second", decimalFormat.format(rate));
        log.info("Average transaction rate pre thread: {} transactions/second", decimalFormat.format(avgRatePerThread));
        log.info("Average transaction rate per instance: {} transactions/second", decimalFormat.format(rate/(float) ports.length));
    }

    private List<List<Integer>> createSlices(int[] ports, int numThreads) {
        final List<List<Integer>> slices = new ArrayList<List<Integer>>(numThreads);
        for (int i=0; i < numThreads; i++) {
            slices.add(new ArrayList<Integer>());
        }
        for (int i=0; i < ports.length;i++) {
            int sliceIdx = i % numThreads;
            slices.get(sliceIdx).add(ports[i]);
        }
        return slices;
    }

    private void doRunPhase(String host, int[] ports) {
        final String data = RandomStringUtils.randomAlphabetic(1024);
        log.info("Opening {} connections", ports.length);
        final MongoClient[] clients = new MongoClient[ports.length];
        for (int i = 0; i < ports.length; i++) {
            clients[i] = new MongoClient(host, ports[i]);
        }

        log.info("Reading {} documents from {} instances", ports.length * 9, ports.length);
        float[] rates = new float[ports.length];
        int id = 0;
        int clientIdx = 0;
        long start = System.currentTimeMillis();
        // read 9 documents per instance
        for (int i = 0; i < ports.length * 9; i++) {
            id = id < 1000 ? id++ : 1;
            final MongoCollection<Document> collection = clients[clientIdx].getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
            final Document toFetch = new Document("_id", id);
            final Document fetched = collection.find(toFetch).first();
            if (fetched == null) {
                log.error("Unable to fetch document with id {}", toFetch.get("_id"));
            }
            if (clientIdx < clients.length - 1) {
                clientIdx++;
            } else {
                clientIdx = 0;
            }
        }

        log.info("Inserting {} documents into {} instances", ports.length, ports.length);
        // write 1 document per instance
        final Document toInsert = new Document()
                .append("data", data);
        for (int i = 0; i < ports.length; i++) {
            final MongoCollection<Document> collection = clients[i].getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
            collection.insertOne(toInsert);
        }
        long duration = System.currentTimeMillis() - start;
        float rate = (float) ports.length * 10000f / (float) duration;
        log.info("Finished {} requests in {} ms with rate {} transactions/sec", ports.length * 10, duration, decimalFormat.format(rate));
        log.info("closing {} connections", clients.length);
        for (MongoClient client : clients) {
            client.close();
        }
    }

    private void doLoadPhase(String host, int[] ports) {
        log.info("Loading data into MongoDB");
        final String data = RandomStringUtils.randomAlphabetic(1024);
        final Document[] docs = new Document[1000];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = new Document()
                    .append("_id", i)
                    .append("data", data);
        }

        // insert the data for each instance of MongoDB
        float[] rates = new float[ports.length];
        for (int i = 0; i < ports.length; i++) {
            long startLoad = System.currentTimeMillis();
            final MongoClient client = new MongoClient(host, ports[i]);
            for (String name : client.listDatabaseNames()) {
                if (name.equalsIgnoreCase(DB_NAME)) {
                    log.warn("Database {} exists and will be purged before inserting", DB_NAME);
                    client.dropDatabase(DB_NAME);
                    break;
                }
            }
            final MongoCollection<Document> collection = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
            collection.insertMany(Arrays.asList(docs));
            client.close();
            long duration = System.currentTimeMillis() - startLoad;
            float rate = 1000f * 1000f / (float) duration;
            rates[i] = rate;
            log.info("Finished loading {} documents in MongoDB on {}:{} in {} ms {} inserts/sec", docs.length, host, ports[i], duration, decimalFormat.format(rate));
        }

        // calculate average
        float avgRate = 0f;
        for (int i = 0; i < rates.length; i++) {
            avgRate += rates[i];
        }
        avgRate = avgRate / rates.length;
        log.info("Load phase achieved an average rate {} inserts/sec", avgRate);
    }

    private static void showHelp(final Options ops) {
        new HelpFormatter().printHelp(80, "mongo-bench", null, ops, null);
    }

}
