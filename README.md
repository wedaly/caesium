Caesium
=======

Experimental system for application monitoring, so named because: caesium is an element used in atomic clocks; clocks are used to measure application response times; this system monitors application response times.


Getting Started
---------------

1. [Install Rust](https://www.rust-lang.org/en-US/install.html), version >= 1.27
2. Start the server: `cargo run --bin server`
3. Start the daemon: `cargo run --bin daemon`

To enable logging to stdout, set the environment variable:
```
RUST_LOG=caesium=debug
```


Inserting and Querying
----------------------

To send response time metrics (via UDP):
```
bash -c "echo -n \"foo:100|ms\" >/dev/udp/127.0.0.1/8001"
```
(this is the same interface as [statsd](https://github.com/etsy/statsd/), so you can use any statsd client library that supports histograms)

The daemon flushes metrics to the backend server in 30 second windows.

To query the server, you can use the `query` command line tool:
```
cargo run --bin query
```

This starts a read-eval-print-loop you can use to query to the server:

| query | meaning |
| ----- | ------- |
| `quantile(fetch(foo), 0.1, 0.5, 0.9)` | Query the 10th, 50th, and 90th percentiles for each time window in the series "foo" |
| `quantile(fetch(foo, 1532646685, 1532651091), 0.5)` | Query the median for windows in a time range |
| `quantile(coalesce(fetch(foo)), 0.5)` | Combine all time windows into one, then query the combined window |
| `quantile(group(hours, fetch(foo)), 0.5)` | Combine time windows that start within the same hour, then query the combined windows |


Measuring Quantile Error
------------------------

Caesium includes a command-line tool for measuring the error introduced by its quantile sketching algorithm.

1. Create a text file containing one number per line (unsigned int u64).  For example:
```
seq 0 100 > data.txt
```

2. Run the `quantile` tool on the data file:
```
cargo run --bin quantile data.txt
```

By default, the quantile tool inserts every value from the data file into a single sketch.  You can measure the error introduced by merging sketches by specifying the number of merges.  For example, to split the dataset into ten sketches that are merged:
```
cargo run --bin quantile data.txt 10
```


Tests
-----

* To run the unit test suite: `cargo test`
* To run the integration test suite: `./tests/run.sh`
* To run performance (micro) benchmarks: `cargo bench`


License
-------
The code in this repository is licensed under version 3 of the AGPL. Please see the LICENSE file for details.
