//
// Created by Maciej Gajek on 25/04/2023.
//

#ifdef __AVX2__
#include <algorithm>

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "util/MurmurHash3.h"
#include "fastfilter_cpp/src/bloom/simd-block-fixed-fpp.h"

namespace leveldb {

template <size_t buckets_div>
class BlockedBloomFilterPolicyFixed : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorPlusFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    auto bloom_filter = SimdBlockFilterFixed64<buckets_div>(n);
    for (int i = 0; i < n; ++i)
      bloom_filter.Add(keyHash(keys[i]));

    auto bucket_bytes = sizeof(typename SimdBlockFilterFixed64<buckets_div>::Bucket) * bloom_filter.bucketCount;

    const size_t init_size = dst->size();
    const size_t additional_size =
      sizeof(int) + // save bucketCount
      sizeof(hashing::SimpleMixSplit) + // save hasher_
      sizeof(size_t) + // save buckets_size_
      bucket_bytes;

    dst->resize(init_size + additional_size, 0);

    auto bucketCount_ptr = (int*) &dst->data()[init_size];
    auto hasher_ptr = (hashing::SimpleMixSplit*) (bucketCount_ptr + 1);
    auto buckets_size_ptr = (size_t*) (hasher_ptr + 1);
    auto buckets_ptr = reinterpret_cast<typename SimdBlockFilterFixed64<buckets_div>::Bucket*> (buckets_size_ptr + 1);

    *bucketCount_ptr = bloom_filter.bucketCount;
    *hasher_ptr = bloom_filter.hasher_;
    *buckets_size_ptr = bucket_bytes;
    std::memcpy(buckets_ptr, bloom_filter.directory_, bucket_bytes);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.empty()) return false;

    auto bucketCount_ptr =  (int*) filter.data();
    auto hasher_ptr = (hashing::SimpleMixSplit*) (bucketCount_ptr + 1);
    auto buckets_size_ptr = (size_t*) (hasher_ptr + 1);
    auto buckets_ptr = reinterpret_cast<typename SimdBlockFilterFixed64<buckets_div>::Bucket*> (buckets_size_ptr + 1);

    SimdBlockFilterFixed64<buckets_div> bloom_filter(
      *bucketCount_ptr,
      *hasher_ptr
    );

    posix_memalign(reinterpret_cast<void**>(&bloom_filter.directory_), 64, *buckets_size_ptr);
    std::memcpy(bloom_filter.directory_, buckets_ptr, *buckets_size_ptr);

    auto res = bloom_filter.Find(keyHash(key));

    return res;
  }
};
#endif //__AVX2__

const FilterPolicy* NewBlockedBloomFilterPolicyFixed(size_t bits_per_key) {
  switch (bits_per_key) {
#ifdef __AVX2__
    case 40:
      return new BlockedBloomFilterPolicyFixed<40>();
    case 41:
      return new BlockedBloomFilterPolicyFixed<41>();
    case 42:
      return new BlockedBloomFilterPolicyFixed<42>();
    case 43:
      return new BlockedBloomFilterPolicyFixed<43>();
    case 44:
      return new BlockedBloomFilterPolicyFixed<44>();
    case 45:
      return new BlockedBloomFilterPolicyFixed<45>();
    case 46:
      return new BlockedBloomFilterPolicyFixed<46>();
    case 47:
      return new BlockedBloomFilterPolicyFixed<47>();
    case 48:
      return new BlockedBloomFilterPolicyFixed<48>();
    case 49:
      return new BlockedBloomFilterPolicyFixed<49>();
    case 50:
      return new BlockedBloomFilterPolicyFixed<50>();
    default:
      return new BlockedBloomFilterPolicyFixed<45>();
#else
    default:
      return nullptr;
#endif //__AVX2__
  }
}

}  // namespace leveldb

