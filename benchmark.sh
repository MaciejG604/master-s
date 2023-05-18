#!/bin/bash

function run_db_bench_fill() {
  local filter="$1"
  local filter_bits="$2"
  shift
  shift

  db_name="${filter_type}_${filter_bits}"

  ./db_bench --benchmarks=fillrandom,overwrite,overwrite,_get_avg_size_,stats \
   --filter_type="$filter" \
    --filter_bits="$filter_bits" \
   --use_existing_db=0 \
   --db=with_"$db_name" \
   --disable_compaction=0 \
   "$@" >> "$db_name".txt
}

function run_db_bench_read() {
  local filter="$1"
  local filter_bits="$2"
  shift
  shift

  db_name="${filter_type}_${filter_bits}"

  ./db_bench --benchmarks=readrandom,readrandom,readrandom,readmissing \
  --filter_type="$filter" \
  --filter_bits="$filter_bits" \
  --use_existing_db=1 \
  --db=with_"$db_name" \
  --disable_compaction=1 \
  "$@" >> "$db_name".txt
}

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 filter_type filter_bits [db_bench options...]"
  exit 1
fi

filter_type="$1"
filter_bits="$2"
shift
shift

filename="${filter_type}_${filter_bits}".txt

printf "\n" > $filename
echo "#Running with filter type: $filter_type with $filter_bits#" >> $filename
printf "\n" >> $filename

run_db_bench_fill "$filter_type" "$filter_bits" "$@"
run_db_bench_read "$filter_type" "$filter_bits" "$@"

echo "#Benchmark done#" >> $filename
printf "\n" >> $filename
