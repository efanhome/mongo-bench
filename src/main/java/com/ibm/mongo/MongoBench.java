/*
 * Copyright (c) 2017, IBM All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
 
package com.ibm.mongo;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
        ops.addOption("a", "record-latencies", true, "Set the file prefix to which to write latencies to");
        ops.addOption("o", "timeout", true, "Set the timeouts in seconds for networking operations");
        ops.addOption("u", "ssl", false, "Use SSL for MongoDB connections");
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
        String latencyFilePrefix;
        int timeouts;
        boolean sslEnabled;

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
                final List<Integer> tmpPorts = new ArrayList<Integer>();
                for (final String range : portVal.split(",")) {
                    int dashIdx = range.indexOf('-');
                    if (dashIdx == -1) {
                        tmpPorts.add(Integer.parseInt(range));
                    } else {
                        int startPort = Integer.parseInt(range.substring(0, dashIdx));
                        int endPort = Integer.parseInt(range.substring(dashIdx + 1));
                        if (endPort < startPort) {
                            throw new ParseException("Port range is invalid. End port must be larger than start port");
                        }
                        for (int i = 0; i <= endPort - startPort; i++) {
                            tmpPorts.add(startPort + i);
                        }
                    }
                }
                ports = new int[tmpPorts.size()];
                for (int i = 0; i < tmpPorts.size(); i++) {
                    ports[i] = tmpPorts.get(i);
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
            if (cli.hasOption('a')) {
                latencyFilePrefix = cli.getOptionValue('a');
            } else {
                latencyFilePrefix = null;
            }
            if (cli.hasOption('o')) {
                timeouts = Integer.parseInt(cli.getOptionValue('o'));
            } else {
                timeouts = 30;
            }
            if (cli.hasOption('u')) {
                sslEnabled = true;
            } else {
                sslEnabled = false;
            }


            log.info("Running phase {}", phase.name());

            final MongoBench bench = new MongoBench();
            if (phase == Phase.LOAD) {
                bench.doLoadPhase(host, ports, numThreads, numDocuments, documentSize, timeouts, sslEnabled);
            } else {
                bench.doRunPhase(host, ports, warmup, duration, numThreads, reportingInterval, rateLimit, latencyFilePrefix, timeouts, sslEnabled);
            }
        } catch (ParseException e) {
            log.error("Unable to parse", e);
        }
    }

    private void doRunPhase(String host, int[] ports, int warmup, int duration, int numThreads, int reportingInterval, float targetRate, String latencyFilePrefix, int timeouts, boolean sslEnabled) {
        log.info("Starting {} threads for {} instances", numThreads, ports.length);
        final Map<RunThread, Thread> threads = new HashMap<RunThread, Thread>(numThreads);
        final List<List<Integer>> slices = createSlices(ports, numThreads);

        for (int i = 0; i < numThreads; i++) {
            RunThread t = new RunThread(host, slices.get(i), targetRate / (float) numThreads, latencyFilePrefix, timeouts, sslEnabled);
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
                decimalFormat.format(maxReadLatency / 1000000f), decimalFormat.format(avgReadLatency / 1000000f));
        log.info("Write latency Min/Max/Avg [ms]: {}/{}/{}", decimalFormat.format(minWriteLatency / 1000000f),
                decimalFormat.format(maxWriteLatency / 1000000f), decimalFormat.format(avgWriteLatency / 1000000f));
    }

    private List<List<Integer>> createSlices(int[] ports, int numThreads) {
        final List<List<Integer>> slices = new ArrayList<List<Integer>>(numThreads);
        if (ports.length >= numThreads) {
        for (int i = 0; i < numThreads; i++) {
            slices.add(new ArrayList<Integer>());
        }
        for (int i = 0; i < ports.length; i++) {
            int sliceIdx = i % numThreads;
            slices.get(sliceIdx).add(ports[i]);
        }
        } else {
            int portIndex = 0;
            for (int i = 0; i < numThreads; i++) {
                final List<Integer> portsTmp;
                if (slices.size() <= i) {
                    portsTmp = new ArrayList<>();
                    slices.add(i, portsTmp);
                } else {
                    portsTmp = slices.get(i);
                }
                portsTmp.add(ports[portIndex++]);
                if (portIndex == ports.length) {
                    portIndex = 0;
                }
            }
        }
        int count = 1;
        for (List<Integer> portTmp : slices) {
            System.out.printf("Thread %d will use ports %s\n", count++, portTmp.toString());
        }
        return slices;
    }


    private void doLoadPhase(String host, int[] ports, int numThreads, int numDocuments, int documentSize, int timeouts, boolean sslEnabled) {
        final Map<LoadThread, Thread> threads = new HashMap<LoadThread, Thread>(numThreads);
        final List<List<Integer>> slices = createSlices(ports, numThreads);
        for (int i = 0; i < numThreads; i++) {
            LoadThread l = new LoadThread(host, slices.get(i), numDocuments, documentSize, timeouts, sslEnabled);
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