//
// Created by Maciej Gajek on 30/03/2023.
//

#include <algorithm>

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "util/MurmurHash3.h"

#include "fastfilter_cpp/src/xorfilter/binaryfusefilter_singleheader.h"

namespace leveldb {

template <typename binary_fuseN_s>
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

    auto filter = binary_fuseN_s {};

    allocate(n, &filter);
    populate(hashed_keys, n, &filter);

    size_t fingerprints_bytes = sizeof(uint8_t) * filter.ArrayLength;;
    if (std::is_same<binary_fuseN_s, binary_fuse16_s>::value)
        fingerprints_bytes *= 2;

    const size_t init_size = dst->size();
    const size_t additional_size = sizeof(binary_fuseN_s) + fingerprints_bytes;
    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (binary_fuseN_s*) &(*dst)[init_size];
    auto fingerprints_ptr = (uint8_t*) &(filter_persist_ptr[1]);

    // save the filter and then the fingerprints
    *filter_persist_ptr = filter;
    std::memcpy(fingerprints_ptr, filter.Fingerprints, fingerprints_bytes);

    delete[] hashed_keys;
    free(&filter);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto filter_persist_pointer = (binary_fuseN_s*) filter.data();
    auto bf_filter = filter_persist_pointer[0];
    auto fingerprints_ptr = (uint8_t*) &filter_persist_pointer[1];

    return contains(keyHash(key), &bf_filter, fingerprints_ptr);
  }

  bool contains(uint64_t key, binary_fuseN_s* filter, uint8_t* fingerprints) const;

  void allocate(uint32_t size, binary_fuseN_s *filter) const;

  void free(binary_fuseN_s *filter) const;

  void populate(const uint64_t *keys, uint32_t size, binary_fuseN_s *filter) const;
};

template<>
bool BinaryFuseFilterPolicy<binary_fuse8_s>::contains(
    uint64_t key,
    binary_fuse8_s* filter,
    uint8_t* fingerprints_ptr) const
{
  filter->Fingerprints = fingerprints_ptr;

  return binary_fuse8_contain(key, filter);
}

template<>
void BinaryFuseFilterPolicy<binary_fuse8_s>::allocate(uint32_t size, binary_fuse8_s *filter) const
{
  binary_fuse8_allocate(size, filter);
}

template<>
void BinaryFuseFilterPolicy<binary_fuse8_s>::free(binary_fuse8_s *filter) const
{
  binary_fuse8_free(filter);
}

template<>
void BinaryFuseFilterPolicy<binary_fuse8_s>::populate(
    const uint64_t *keys,
    uint32_t size,
    binary_fuse8_s *filter) const
{
  binary_fuse8_populate(keys, size, filter);
}

template<>
bool BinaryFuseFilterPolicy<binary_fuse16_s>::contains(
    uint64_t key,
    binary_fuse16_s* filter,
    uint8_t* fingerprints_ptr) const
{
  filter->Fingerprints = (uint16_t*) fingerprints_ptr;

  return binary_fuse16_contain(key, filter);
}

template<>
void BinaryFuseFilterPolicy<binary_fuse16_s>::allocate(uint32_t size, binary_fuse16_s *filter) const
{
  binary_fuse16_allocate(size, filter);
}

template<>
void BinaryFuseFilterPolicy<binary_fuse16_s>::free(binary_fuse16_s *filter) const
{
  binary_fuse16_free(filter);
}

template<>
void BinaryFuseFilterPolicy<binary_fuse16_s>::populate(const uint64_t *keys, uint32_t size, binary_fuse16_s *filter) const
{
  binary_fuse16_populate(keys, size, filter);
}

const FilterPolicy* NewBinaryFuseFilterPolicy(size_t bits_per_key) {
  switch (bits_per_key) {
    case 16:
      return new BinaryFuseFilterPolicy<binary_fuse16_s>();
    default:
      return new BinaryFuseFilterPolicy<binary_fuse8_s>();
  }
}

}  // namespace leveldb