//
// Created by Maciej Gajek on 30/03/2023.
//

#include <iostream>
#include <memory>

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "util/MurmurHash3.h"
//#include "util/filter_adapters/ribbon/ribbon_serialization.h"
#include "util/filter_adapters/ribbon/BalancedRibbonFilter.h"

namespace leveldb {

class RibbonFilterPolicy : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
//    Standard128RibbonBitsBuilder builder(0.03, true);
//
//    for (int i = 0; i < n; ++i) {
//      builder.AddKey(keyHash(keys[i]));
//    }
//
//    const size_t init_size = dst->size();
//    const size_t additional_size = builder.CalculateSpace(n);
//
//    dst->resize(init_size + additional_size, 0);
//
//    std::unique_ptr<const char[]> buffer(&(*dst)[init_size]);
//
//    builder.Finish(&buffer);
    if (n > 0) {
      BalancedRibbonFilter<uint64_t, 8, 0> filter(n);

      std::vector<uint64_t> vec(n);

      for (int i = 0; i < n; ++i) {
        vec[i] = keyHash(keys[i]);
      }

      filter.AddAll(vec, 0, vec.size());

      using SolnT = BalancedRibbonFilter<uint64_t, 8, 0>::InterleavedSoln;

      const size_t init_size = dst->size();
      const size_t additional_size =
          sizeof(uint32_t) + 2* sizeof(size_t) +
          sizeof(SolnT) +
          filter.SizeInBytes();

      dst->resize(init_size + additional_size, 0);

      uint32_t* log2_vshards_ptr = (uint32_t*)&(*dst)[init_size];
      SolnT* soln_ptr = (SolnT*) (log2_vshards_ptr + 1);
      size_t* num_slots_ptr = (size_t*) (soln_ptr + 1);
      size_t* meta_len_ptr = num_slots_ptr + 1;
      uint8_t* meta_ptr = (uint8_t*)meta_len_ptr + sizeof(size_t);
      uint8_t* bytes_ptr = meta_ptr + filter.meta_bytes;

      *log2_vshards_ptr = filter.log2_vshards;
      std::memcpy(soln_ptr, &filter.soln, sizeof(SolnT));
      *num_slots_ptr = filter.num_slots;
      *meta_len_ptr = filter.meta_bytes;
      std::memcpy(meta_ptr, filter.meta_ptr.get(), filter.meta_bytes);
      std::memcpy(bytes_ptr, filter.ptr.get(), filter.bytes);
    }
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
//    uint32_t seed = filter[filter.size() - 4];
//    uint32_t num_blocks = *((uint32_t*) filter.data()+ filter.size() - 3);
//
//    Standard128RibbonBitsReader reader(filter.data(), filter.size(), num_blocks, seed);
//
//    return reader.HashMayMatch(keyHash(key));
    if (filter.empty())
      return false;

    using SolnT = BalancedRibbonFilter<uint64_t, 8, 0>::InterleavedSoln;

    uint32_t* log2_vshards_ptr = (uint32_t*) filter.data();
    SolnT* soln_ptr = (SolnT*) (log2_vshards_ptr + 1);
    size_t* num_slots_ptr = (size_t*) (soln_ptr + 1);
    size_t* meta_len_ptr = num_slots_ptr + 1;

    auto meta_len = *meta_len_ptr;

    char* meta_ptr = (char*) meta_len_ptr + sizeof(size_t);
    char* bytes_ptr = meta_ptr + meta_len;

    BalancedRibbonFilter<uint64_t, 8, 0> ribbon(
        *log2_vshards_ptr,
        *num_slots_ptr,
        bytes_ptr,
        filter.size() - (sizeof(uint32_t) + 2* sizeof(size_t) + sizeof(SolnT) + meta_len),
        meta_ptr,
        meta_len
        );

    ribbon.soln.num_starts_ = soln_ptr->num_starts_;
    ribbon.soln.upper_num_columns_ = soln_ptr->upper_num_columns_;
    ribbon.soln.upper_start_block_ = soln_ptr->upper_start_block_;

    auto res = ribbon.Contain(keyHash(key));

    ribbon.meta_ptr.release();
    ribbon.ptr.release();

    return res;
  }


};

const FilterPolicy* NewRibbonFilterPolicy() {
  return new RibbonFilterPolicy();
}

}  // namespace leveldb