#!/usr/bin/bash

function run_db_bench_fill() {
  local filter="$1"
  shift
  ./db_bench --benchmarks=fillrandom,overwrite,overwrite,compact,stats --filter_type="$filter" --use_existing_db=0 --db=with_"$filter" "$@" > "$filter".txt
}

function run_db_bench_read() {
  local filter="$1"
  shift
  ./db_bench --benchmarks=readrandom,readmissing --filter_type="$filter" --use_existing_db=1 --db=with_"$filter" "$@" >> "$filter".txt
}

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 filter_type [db_bench options...]"
  exit 1
fi

filter_type="$1"
shift

run_db_bench_fill "$filter_type" "$@"
run_db_bench_read "$filter_type" "$@"

