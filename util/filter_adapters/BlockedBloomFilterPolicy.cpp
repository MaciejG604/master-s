////
//// Created by Maciej Gajek on 25/04/2023.
////
//
//#include <iostream>
//#include <memory>
//
//#include "leveldb/filter_policy.h"
//#include "leveldb/slice.h"
//
//#include "util/MurmurHash3.h"
////#include "util/filter_adapters/ribbon/ribbon_serialization.h"
//#include "util/filter_adapters/ribbon/BalancedRibbonFilter.h"
//#include "util/filter_adapters/XorFilterPolicy.h"
//
//namespace leveldb {
//
//class BlockedBloomFilterPolicy : public FilterPolicy {
// public:
//
//  explicit BlockedBloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
//    // We intentionally round down to reduce probing cost a little bit
//    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
//    if (k_ < 1) k_ = 1;
//    if (k_ > 30) k_ = 30;
//  }
//
//  static uint64_t keyHash(const leveldb::Slice& key) {
//
//    uint64_t hash_value = 0;
//
//    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);
//
//    return hash_value;
//  }
//
//  const char* Name() const override { return "XorFilterPolicy"; }
//
//  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
//
//    }
//  }
//
//  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
//    if (filter.empty())
//      return false;
//
//
//  }
//
//  int bits_per_key_;
//};
//
//const FilterPolicy* NewBlockedBloomFilterPolicy() {
//  return new BlockedBloomFilterPolicy();
//}
//
//}  // namespace leveldb
