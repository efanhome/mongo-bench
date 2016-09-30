package com.ibm.mongo;

import com.mongodb.MongoClient;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;

public class MongoBench {

    private static final Logger log = LoggerFactory.getLogger(MongoBench.class);

    public final static String DB_NAME = "mongo-bench";

    public final static String COLLECTION_NAME = "mongo-bench-documents";

    private final static DecimalFormat decimalFormat = new DecimalFormat("0.0000");

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
        ops.addOption("r", "reporting-interval", true, "The interval in seconds for reporting progress");
        ops.addOption("c", "num-documents", true, "The number of documents to create during the load phase");
        ops.addOption("s", "document-size", true, "The size of the created documents");
        ops.addOption("w", "warmup-time", true, "The number of seconds to wait before actually collecting result data");
        ops.addOption("j", "target-rate", true, "Send request at the given rate. Accepts decimal numbers");
        ops.addOption("h", "help", false, "Show this help dialog");

        final CommandLineParser parser = new DefaultParser();
        final Phase phase;
        final int[] ports;
        final String host;
        int duration;
        int numThreads;
        int reportingInterval;
        int documentSize;
        int numDocuments;
        int warmup;
        float rateLimit;

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
                throw new ParseException("No phase given. Try \"--help/-h\"");
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
                    if (endPort < startPort) {
                        throw new ParseException("Port range is invalid. End port must be larger than start port");
                    }
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
            if (cli.hasOption('r')) {
                reportingInterval = Integer.parseInt(cli.getOptionValue('r'));
            } else {
                reportingInterval = 60;
            }
            if (cli.hasOption('c')) {
                numDocuments = Integer.parseInt(cli.getOptionValue('c'));
            } else {
                numDocuments = 1000;
            }
            if (cli.hasOption('s')) {
                documentSize = Integer.parseInt(cli.getOptionValue('s'));
            } else {
                documentSize = 1024;
            }
            if (cli.hasOption('w')) {
                warmup = Integer.parseInt(cli.getOptionValue('w'));
            } else {
                warmup = 0;
            }
            if (cli.hasOption('j')) {
                rateLimit = Float.parseFloat(cli.getOptionValue('j'));
            } else {
                rateLimit = 0f;
            }

            log.info("Running phase {}", phase.name());

            final MongoBench bench = new MongoBench();
            if (phase == Phase.LOAD) {
                bench.doLoadPhase(host, ports, numThreads, numDocuments, documentSize);
            } else {
                if (numThreads > ports.length) {
                    throw new ParseException("Number of threads must be smaller than number of ports");
                }
                bench.doRunPhase(host, ports, warmup, duration, numThreads, reportingInterval, rateLimit);
            }
        } catch (ParseException e) {
            log.error("Unable to parse", e);
        }
    }

    private void doRunPhase(String host, int[] ports, int warmup, int duration, int numThreads, int reportingInterval, float targetRate) {
        log.info("Starting {} threads for {} instances", numThreads, ports.length);
        final Map<RunThread, Thread> threads = new HashMap<RunThread, Thread>(numThreads);
        final List<List<Integer>> slices = createSlices(ports, numThreads);
        for (int i = 0; i < numThreads; i++) {
            RunThread t = new RunThread(host, slices.get(i), targetRate/(float) numThreads);
            threads.put(t, new Thread(t));
        }
        for (final Thread t : threads.values()) {
            t.start();
        }

        for (final RunThread r : threads.keySet()) {
            while (!r.isInitialized()) {
                Thread.yield();
            }
        }
        log.info("Client threads have been initialized");

        // run the warmup phase id a warmup greater than 0 has been passed by the user
        warmup(warmup);

        for (RunThread r : threads.keySet()) {
            r.resetData();
        }

        long start = System.currentTimeMillis();
        long lastInterval = start;
        long currentMillis = System.currentTimeMillis();
        long interval;
        while ((interval = currentMillis - start) < 1000 * duration) {
            if (currentMillis - lastInterval > reportingInterval * 1000) {
                collectAndReportLatencies(threads.keySet(), interval);
                lastInterval = currentMillis;
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                log.error("Unable to sleep", e);
            }
            currentMillis = System.currentTimeMillis();
        }
        for (RunThread r : threads.keySet()) {
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
        log.info("Average transaction rate per instance: {} transactions/second", decimalFormat.format(rate / (float) ports.length));
        collectAndReportLatencies(threads.keySet(), elapsed);
        collectAndReport90PLatency(threads.keySet());
    }

    private void warmup(int warmupInSeconds) {
        if (warmupInSeconds > 0) {
            long startWarmup = System.currentTimeMillis();
            log.info("Warm up for {} seconds", warmupInSeconds);
            while (System.currentTimeMillis() - startWarmup < warmupInSeconds * 1000) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.info("Warmup finished.");
        }
    }

    private void collectAndReport90PLatency(Set<RunThread> runThreads) {
        int countReadLatencies = 0, countWriteLatencies = 0;
        for (final RunThread r : runThreads) {
            countReadLatencies += r.getReadLatencies().size();
            countWriteLatencies += r.getWriteLatencies().size();
        }
        final long[] readLatencies = new long[countReadLatencies];
        final long[] writeLatencies = new long[countWriteLatencies];
        for (final RunThread r : runThreads) {
            int idx = 0;
            for (final Long latency : r.getReadLatencies()) {
                readLatencies[idx++] = latency;
            }
            idx = 0;
            for (final Long latency : r.getWriteLatencies()) {
                writeLatencies[idx++] = latency;
            }
        }
        Arrays.sort(readLatencies);
        Arrays.sort(writeLatencies);
        final long read90PLatency = readLatencies[(int) 0.9f * countReadLatencies];
        final long write90PLatency = writeLatencies[(int) 0.9f * countWriteLatencies];
        final long read50PLatency = readLatencies[(int) 0.5f * countReadLatencies];
        final long write50PLatency = writeLatencies[(int) 0.5f * countWriteLatencies];
        log.info("Read Latencies 90 percentile/50 Percentile [ms]: {}/{}", decimalFormat.format(read90PLatency / 1000f), decimalFormat.format(read50PLatency / 1000000f));
        log.info("Write Latencies 90 percentile/50 Percentile [ms]: {}/{}", decimalFormat.format(write90PLatency / 1000f), decimalFormat.format(write50PLatency / 1000000f));
    }

    private void collectAndReportLatencies(Set<RunThread> threads, long duration) {
        int numInserts = 0, numReads = 0;
        float minReadLatency = Float.MAX_VALUE, maxReadLatency = 0f, minWriteLatency = Float.MAX_VALUE, maxWriteLatency = 0f;
        float avgReadLatency = 0f, avgWriteLatency = 0f;
        float tps;
        for (final RunThread r : threads) {
            numReads += r.getNumReads();
            numInserts += r.getNumInserts();
            if (r.getMaxReadlatency() > maxReadLatency) {
                maxReadLatency = r.getMaxReadlatency();
            }
            if (r.getMaxWriteLatency() > maxWriteLatency) {
                maxWriteLatency = r.getMaxWriteLatency();
            }
            if (r.getMinReadLatency() < minReadLatency) {
                minReadLatency = r.getMinReadLatency();
            }
            if (r.getMinWriteLatency() < minWriteLatency) {
                minWriteLatency = r.getMinWriteLatency();
            }
            avgReadLatency += r.getAccReadLatencies();
            avgWriteLatency += r.getAccWriteLatencies();
        }
        avgReadLatency = avgReadLatency / numReads;
        avgWriteLatency = avgWriteLatency / numInserts;
        tps = (numInserts + numReads) * 1000f / (duration);
        log.info("{} inserts, {} reads in {} s, {} requests/sec", numInserts, numReads, decimalFormat.format(duration / 1000f), decimalFormat.format(tps));
        log.info("Read latency Min/Max/Avg [ms]: {}/{}/{}", decimalFormat.format(minReadLatency / 1000000f),
                decimalFormat.format(maxReadLatency / 100000f), decimalFormat.format(avgReadLatency / 1000000f));
        log.info("Write latency Min/Max/Avg [ms]: {}/{}/{}", decimalFormat.format(minWriteLatency / 1000000f),
                decimalFormat.format(maxWriteLatency / 1000000f), decimalFormat.format(avgWriteLatency / 1000000f));
    }

    private List<List<Integer>> createSlices(int[] ports, int numThreads) {
        final List<List<Integer>> slices = new ArrayList<List<Integer>>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            slices.add(new ArrayList<Integer>());
        }
        for (int i = 0; i < ports.length; i++) {
            int sliceIdx = i % numThreads;
            slices.get(sliceIdx).add(ports[i]);
        }
        return slices;
    }


    private void doLoadPhase(String host, int[] ports, int numThreads, int numDocuments, int documentSize) {
        if (numThreads > ports.length) {
            log.error("The number of threads ({}) can not be larger than the number of ports ({})", numThreads, ports.length);
            return;
        }
        Map<LoadThread, Thread> threads = new HashMap<LoadThread, Thread>(numThreads);
        final List<List<Integer>> portSlice = new ArrayList<List<Integer>>(numThreads);
        for (int i = 0; i < ports.length; i++) {
            int idx = i % numThreads;
            if (portSlice.size() < idx + 1) {
                portSlice.add(idx, new ArrayList<Integer>());
            }
            portSlice.get(idx).add(ports[i]);
        }
        for (int i = 0; i < numThreads; i++) {
            LoadThread l = new LoadThread(host, portSlice.get(i), numDocuments, documentSize);
            threads.put(l, new Thread(l));
        }

        for (Thread t : threads.values()) {
            t.start();
        }
        for (Thread t : threads.values()) {
            if (t.isAlive()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("Error while waiting for thread", e);
                }
            }
        }
    }

    private static void showHelp(final Options ops) {
        final StringBuilder header = new StringBuilder();
        header.append("\nOptions:");
        final StringBuilder footer = new StringBuilder();
        footer.append("\nThe benchmark is split into two phases: Load and Run. ")
                .append("Random data is added during the load phase which is in turn retrieved from mongodb in the run phase");
        String jarName;
        try {
            jarName = MongoBench.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
            int posSlash = jarName.lastIndexOf('/');
            if (posSlash != -1 && jarName.length() > posSlash) {
                jarName = jarName.substring(posSlash + 1);
            }
        } catch (URISyntaxException e) {
            jarName = "mongo-bench.jar";
        }
        new HelpFormatter().printHelp(120, "java -jar " + jarName + " [options]", header.toString(), ops, footer.toString());
    }

}
