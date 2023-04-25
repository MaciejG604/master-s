//
// Created by Maciej Gajek on 30/03/2023.
//

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "fastfilter_cpp/src/xorfilter/binaryfusefilter_singleheader.h"
#include "util/MurmurHash3.h"

namespace leveldb {

class BinaryFuseFilterPolicy : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "BinaryFuseFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    auto* hashed_keys = new uint64_t[n];
    std::transform(keys, &keys[n], hashed_keys, [](const Slice& x){return keyHash(x);});

    auto filter = binary_fuse8_t {};
    binary_fuse8_allocate(n, &filter);
    binary_fuse8_populate(hashed_keys, n, &filter);

    const size_t fingerprints_bytes = filter.ArrayLength;

    const size_t init_size = dst->size();
    const size_t additional_size = sizeof(binary_fuse8_t) + fingerprints_bytes;
    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (binary_fuse8_t*) &(*dst)[init_size];
    auto fingerprints_ptr = (uint8_t*) &(filter_persist_ptr[1]);

    // save the filter and then the fingerprints
    *filter_persist_ptr = filter;
    std::memcpy(fingerprints_ptr, filter.Fingerprints, fingerprints_bytes);

    delete[] hashed_keys;
    binary_fuse8_free(&filter);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto filter_persist_pointer = (binary_fuse8_t*) filter.data();
    auto bf_filter = filter_persist_pointer[0];
    auto fingerprints_ptr = (uint8_t*) &filter_persist_pointer[1];

    bf_filter.Fingerprints = fingerprints_ptr;

    return binary_fuse8_contain(keyHash(key), &bf_filter);
  }
};

const FilterPolicy* NewBinaryFuseFilterPolicy() {
  return new BinaryFuseFilterPolicy();
}

}  // namespace leveldb