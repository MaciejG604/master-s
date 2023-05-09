//
// Created by Maciej Gajek on 25/04/2023.
//

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#ifdef __AVX2__

#include <iostream>
#include <algorithm>
#include <memory>

#include "util/MurmurHash3.h"
#include "fastfilter_cpp/src/bloom/simd-block.h"

namespace leveldb {

class BlockedBloomFilterPolicy : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorPlusFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    auto bloom_filter = SimdBlockFilter<>(ceil(log2(n)));
    for (int i = 0; i < n; ++i)
      bloom_filter.Add(keyHash(keys[i]));

    auto bucket_bytes = (1ull << (bloom_filter.log_num_buckets_ + bloom_filter.LOG_BUCKET_BYTE_SIZE));

    const size_t init_size = dst->size();
    const size_t additional_size =
      sizeof(int) + // save log_num_buckets_
      sizeof(uint32_t) + // save directory_mask_
      sizeof(hashing::SimpleMixSplit) + // save hasher_
      sizeof(size_t) + // save buckets_size_
      bucket_bytes;

    dst->resize(init_size + additional_size, 0);

    auto log_num_buckets_ptr = (int*) &dst->data()[init_size];
    auto directory_mask_ptr = (uint32_t*) (log_num_buckets_ptr + 1);
    auto hasher_ptr = (hashing::SimpleMixSplit*) (directory_mask_ptr + 1);
    auto buckets_size_ptr = (size_t*) (hasher_ptr + 1);
    auto buckets_ptr = (SimdBlockFilter<>::Bucket*) (buckets_size_ptr + 1);

    *log_num_buckets_ptr = bloom_filter.log_num_buckets_;
    *directory_mask_ptr = bloom_filter.directory_mask_;
    *hasher_ptr = bloom_filter.hasher_;
    *buckets_size_ptr = bucket_bytes;
    std::memcpy(buckets_ptr, bloom_filter.directory_, bucket_bytes);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.empty()) return false;

    auto log_num_buckets_ptr = (int*) filter.data();
    auto directory_mask_ptr = (uint32_t*) (log_num_buckets_ptr + 1);
    auto hasher_ptr = (hashing::SimpleMixSplit*) (directory_mask_ptr + 1);
    auto buckets_size_ptr = (size_t*) (hasher_ptr + 1);
    auto buckets_ptr = (SimdBlockFilter<>::Bucket*) (buckets_size_ptr + 1);

    SimdBlockFilter<> bloom_filter(
      *log_num_buckets_ptr,
      *directory_mask_ptr,
      *hasher_ptr
    );

    posix_memalign(reinterpret_cast<void**>(&bloom_filter.directory_), 64, *buckets_size_ptr);
    std::memcpy(bloom_filter.directory_, buckets_ptr, *buckets_size_ptr);

    auto res = bloom_filter.Find(keyHash(key));

    return res;
  }
};

#endif //__AVX2__

const leveldb::FilterPolicy* NewBlockedBloomFilterPolicy(size_t bits_per_key) {
#ifdef __AVX2__
  return new BlockedBloomFilterPolicy();
#else
  return nullptr;
#endif //__AVX2__
}

#ifdef __AVX2__
}  // namespace leveldb
#endif //__AVX2__
