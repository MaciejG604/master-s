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