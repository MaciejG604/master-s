#!/usr/bin/env bash

sed -i '' 's/static int FLAGS_bloom_bits = [-0-9]*;/static int FLAGS_bloom_bits = -1;/' ../benchmarks/db_bench.cc &&
cmake -DCMAKE_BUILD_TYPE=Release .. > /dev/null && cmake --build . > /dev/null &&
./db_bench > no_bloom.txt
echo "No bloom DONE"
sed -i '' 's/static int FLAGS_bloom_bits = [-0-9]*;/static int FLAGS_bloom_bits = 7;/' ../benchmarks/db_bench.cc &&
cmake -DCMAKE_BUILD_TYPE=Release .. > /dev/null && cmake --build . > /dev/null&&
./db_bench > with_bloom.txt
echo "With bloom DONE"
echo "run diff no_bloom.txt with_bloom.txt"

# ./db_bench --benchmarks=fillrandom,overwrite,overwrite,compact,stats --filter_type=2 --use_existing_db=0 --db=/Users/mgajek/CLionProjects/leveldb/build/with_xor
# ./db_bench --benchmarks=readrandom,readmissing --filter_type=xor --use_existing_db=1 --db=/Users/mgajek/CLionProjects/leveldb/build/with_xor > xor.txt

function run_db_bench_fill() {
  local filter="$1"
  ./db_bench --benchmarks=fillrandom,overwrite,overwrite,compact,stats --filter_type="$filter" --use_existing_db=0 --db=/Users/mgajek/CLionProjects/leveldb/build/with_"$filter" > "$filter".txt
}

function run_db_bench_read() {
  local filter="$1"
  ./db_bench --benchmarks=readrandom,readmissing --filter_type="$filter" --use_existing_db=1 --db=/Users/mgajek/CLionProjects/leveldb/build/with_"$filter" >> "$filter".txt
}



