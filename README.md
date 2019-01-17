Caesium
=======

Experimental system for application monitoring, so named because: caesium is an element used in atomic clocks; clocks are used to measure application response times; this system monitors application response times.

Getting Started
---------------

Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/).

To start the server and daemon locally:
```
docker-compose up
```

This will also start a load testing program that inserts one metric per second to the daemon.


Inserting and Querying
----------------------

To send response time metrics (via UDP):
```
bash -c "echo -n \"foo:100|ms\" >/dev/udp/127.0.0.1/8001"
```
(this is the same interface as [statsd](https://github.com/etsy/statsd/), so you can use any statsd client library that supports histograms)

The daemon flushes metrics to the backend server in 30 second windows.

To query the server, you can use the `caesium-query` command line tool:
```
docker-compose run cli caesium-query
```

This starts a read-eval-print-loop you can use to query to the server:

| query | meaning |
| ----- | ------- |
| `quantile(fetch("foo"), 0.1, 0.5, 0.9)` | Query the 10th, 50th, and 90th percentiles for each time window in the series "foo" |
| `quantile(fetch("foo", 1532646685, 1532651091), 0.5)` | Query the median for windows in a time range |
| `quantile(coalesce(fetch("foo")), 0.5)` | Combine all time windows into one, then query the combined window |
| `quantile(group("hours", fetch("foo")), 0.5)` | Combine time windows that start within the same hour, then query the combined windows |
| `quantile(combine(fetch("foo"), fetch("bar")), 0.5)` | Combine overlapping time windows from "foo" and "bar", then query the median of each window |


Measuring Quantile Error
------------------------

Caesium includes a command-line tool for measuring the error introduced by its quantile sketching algorithm.

Example:
```
docker-compose run cli sh
$ seq 0 100 > data.txt
$ caesium-quantile -s -e data.txt
```

This will report:
* The *normalized rank error* for the 0.1, 0.2, .., 0.8, 0.9 quantiles
* The number of values stored in the sketch.
* The size of the (serialized) sketch in bytes.
* The total time in ms to perform the inserts/merges.

By default, the quantile tool inserts every value from the data file into a single sketch.  You can measure the error introduced by merging sketches by specifying the number of merges.  For example, to split the dataset into ten sketches that are merged:
```
$ caesium-quantile data.txt -n 10
```


Building Locally
----------------

1. [Install Rust](https://www.rust-lang.org/en-US/install.html), version >= 1.32
2. Build the project: `cargo build`
3. Binaries will be written to the "target" directory.


Tests
-----

* To run the test suite: `cargo test`
* To run performance (micro) benchmarks: `cargo bench`


License
-------
The code in this repository is licensed under version 3 of the AGPL. Please see the LICENSE file for details.
