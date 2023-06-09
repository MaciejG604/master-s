//
// Created by Maciej Gajek on 25/04/2023.
//

#ifndef LEVELDB_XORFILTERPOLICY_H
#define LEVELDB_XORFILTERPOLICY_H

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "util/MurmurHash3.h"

namespace leveldb {

template <typename xorN_s>
class XorFilterPolicy : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override;

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override;

 private:
  bool contains(uint64_t key, xorN_s* filter, uint8_t* fingerprints) const;

  void allocate(uint32_t size, xorN_s *filter) const;

  void free(xorN_s *filter) const;

  void populate(const uint64_t *keys, uint32_t size, xorN_s *filter) const;
};

}  // namespace leveldb

#endif  // LEVELDB_XORFILTERPOLICY_H
