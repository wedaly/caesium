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
    RUST_LOG=caesium=debug $BIN/server testdb &
    sleep 1
}

function insert {
    $BIN/insert $1 $2 $3
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
insert "m1" 1 $DATA/one_to_ten.txt
insert "m2" 1 $DATA/one_to_ten.txt
insert "m2" 2 $DATA/ten_to_twenty.txt
query "quantile(0.5, fetch(m1))" "m1_median.txt"
query "quantile(0.5, fetch(m2))" "m2_median.txt"
check "m1_median.txt"
check "m2_median.txt"
cleanup
echo "ALL TESTS PASSED!"
