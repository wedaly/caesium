#!/usr/bin/env bash

set -x
set -e

SCRIPT=`readlink -f "$0"`
TESTS_DIR=`dirname "$SCRIPT"`
ROOT_DIR=`dirname "$TESTS_DIR"`
TEST_DB=testdb
BIN=$ROOT_DIR/target/debug
DATA=$TESTS_DIR/data
OUT=$TESTS_DIR/out
EXPECTED=$TESTS_DIR/expected

function setup {
    rm -rf $TEST_DB
    rm -rf $OUT
    mkdir -p $OUT
}

function cleanup {
    rm -rf $TEST_DB
}

function background_server {
    RUST_LOG=caesium=debug $BIN/server --db-name testdb &
    sleep 1
}

function insert {
    $BIN/insert $1 $2 $3 $4
}

function query {
    echo $1 | $BIN/query > $OUT/$2
}

function check {
    diff $EXPECTED/$1 $OUT/$1
}

trap 'kill $(jobs -p)' EXIT

########## TEST ###########
setup
background_server
insert "m1" 30 60 $DATA/one_to_ten.txt
insert "m2" 30 60 $DATA/one_to_ten.txt
insert "m2" 60 90 $DATA/ten_to_twenty.txt
insert "m3" 10 20 $DATA/one_to_ten.txt
insert "m3" 20 30 $DATA/ten_to_twenty.txt
insert "m3" 50 100 $DATA/ten_to_twenty.txt
insert "m4" 10 20 $DATA/one_to_ten.txt
insert "m4" 15 40 $DATA/ten_to_twenty.txt
insert "m5" 10 20 $DATA/one_to_ten.txt
insert "m5" 15 40 $DATA/ten_to_twenty.txt
insert "m6" 50 60 $DATA/one_to_ten.txt
insert "m6" 60 70 $DATA/ten_to_twenty.txt
insert "m6" 4000 5000 $DATA/one_to_ten.txt
insert "m6" 5500 6000 $DATA/ten_to_twenty.txt
insert "m7" 30 60 $DATA/one_to_ten.txt
query "quantile(fetch(m1), 0.5)" "m1_median.txt"
query "quantile(fetch(m2), 0.5)" "m2_median.txt"
query "quantile(coalesce(fetch(m3)), 0.5)" "m3_median.txt"
query "quantile(combine(fetch(m4), fetch(m5)), 0.5)" "m4_m5_median.txt"
query "quantile(group(hours, fetch(m6)), 0.5)" "m6_hours_median.txt"
query "quantile(group(days, fetch(m6)), 0.5)" "m6_days_median.txt"
query "quantile(fetch(m7), 0.1, 0.5, 0.9)" "m7_multiple_quantiles.txt"
query "search(m*)" "search_metrics.txt"
check "m1_median.txt"
check "m2_median.txt"
check "m3_median.txt"
check "m4_m5_median.txt"
check "m6_hours_median.txt"
check "m6_days_median.txt"
check "m7_multiple_quantiles.txt"
check "search_metrics.txt"
cleanup
echo "ALL TESTS PASSED!"
