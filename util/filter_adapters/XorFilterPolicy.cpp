//
// Created by Maciej Gajek on 30/03/2023.
//

#include "XorFilterPolicy.h"

#include "fastfilter_cpp/src/xorfilter/xorfilter_singleheader.h"

namespace leveldb {

bool XorFilterPolicy::KeyMayMatch(const Slice& key, const Slice& filter) const {
  if (filter.size() <= 0)
    return false;

  uint8_t* fingerprints_ptr = ((uint8_t*) filter.data()) + sizeof(uint64_t);
  uint64_t seed = ((uint64_t *) filter.data())[0];

  xor8_t xor_filter = xor8_t {
      seed,
      (filter.size() - sizeof(uint64_t)) / 3,
      fingerprints_ptr
  };

  return xor8_contain(keyHash(key), &xor_filter);
}
void XorFilterPolicy::CreateFilter(const leveldb::Slice* keys, int n,
                                   std::string* dst) const {
  auto* hashed_keys = new uint64_t[n];
  std::transform(keys, &keys[n], hashed_keys, [](const Slice& x){return keyHash(x);});

  xor8_t xor_filter = xor8_t {};
  xor8_allocate(n, &xor_filter);
  xor8_populate(hashed_keys, n, &xor_filter);

  const size_t fingerprints_bytes = xor_filter.blockLength * 3;

  const size_t init_size = dst->size();
  const size_t additional_size = sizeof(uint64_t) + fingerprints_bytes;
  dst->resize(init_size + additional_size, 0);

  uint64_t* seed_ptr = (uint64_t*) &(*dst)[init_size];
  uint8_t* fingerprints_ptr = (uint8_t*) &(*dst)[init_size] + sizeof(uint64_t);

  // save the seed and then the fingerprints
  *seed_ptr = xor_filter.seed;
  std::memcpy(fingerprints_ptr, xor_filter.fingerprints, fingerprints_bytes);

  xor8_free(&xor_filter);
  delete[] hashed_keys;
}

const FilterPolicy* NewXorFilterPolicy() {
  return new XorFilterPolicy();
}

}  // namespace leveldb