# Tiny MongoDB Benchmark

This is a benchmark for assessing MongoDB performance in a SaaS environment when running a lot of instances on a single box/VM. The benchmark is split into a load and a run phase. During the load phase, random data is inserted into MongoDB, during the run phase the data is retrieved and new random data is inserted into MongoDB with a read/write ratio of 0.9.

## Building

The benchmark is a Java project build with Apache Maven. In order to build the project from source run the package phase of Apache Maven:
```bash
#> mvn package
```

## Usage

The benchmark is written in Java and at least Java 6 is required to run the generated jar file. Since a lot of latency data is recorded per thread large amounts of memory are needed and the benchmark has to be run with a larger than normal heapsize during the run phase

    usage: java -jar mongo-bench-1.0-SNAPSHOT-jar-with-dependencies.jar [options]

    Options:
     -c,--num-documents <arg>        The number of documents to create during the load phase
     -d,--duration <arg>             Run the bench for this many seconds
     -h,--help                       Show this help dialog
     -j,--target-rate <arg>          Send request at the given rate. Accepts decimal numbers
     -l,--phase <arg>                The phase to execute [run|load]
     -n,--num-thread <arg>           The number of threads to run
     -p,--port <arg>                 The ports to connect to
     -r,--reporting-interval <arg>   The interval in seconds for reporting progress
     -s,--document-size <arg>        The size of the created documents
     -t,--target  <arg>              The target host to connect to
     -w,--warmup-time <arg>          The number of seconds to wait before actually collecting result data

    The benchmark is split into two phases: Load and Run. Random data is added during the load phase which is in turn
    retrieved from mongodb in the run phase


### Examples
Loading `1000` documents of size `1024` bytes each into MongoDB instances on ports `30001-30010` on `9.114.14.14` using `8` threads:
```bash
#> java -jar /tmp/mongo-bench-1.0-SNAPSHOT-jar-with-dependencies.jar -s 1024 -c 1000 -l load -p 30001-30010 -t 9.114.14.14 -n 8
```


Running the benchmark against ports `30001-30010` on the box `9.114.14.14` using 4 threads for `600` seconds with a `60` second warmup time and target an overall rate of `1000` transactions/second:
```bash
#> java -Xmx16384m -jar /tmp/mongo-bench-1.0-SNAPSHOT-jar-with-dependencies.jar -l run -p 30001-30010 -t 9.114.14.14 -n 4 -w 60 -d 600 -j 1000
```