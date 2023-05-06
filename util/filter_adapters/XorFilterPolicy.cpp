//
// Created by Maciej Gajek on 30/03/2023.
//

#include "XorFilterPolicy.h"

#include <algorithm>

#include "fastfilter_cpp/src/xorfilter/xorfilter_singleheader.h"

namespace leveldb {

template <typename xorN_s>
void XorFilterPolicy<xorN_s>::CreateFilter(const leveldb::Slice* keys, int n,
                                           std::string* dst) const {
  auto* hashed_keys = new uint64_t[n];
  std::transform(keys, &keys[n], hashed_keys, [](const Slice& x){return keyHash(x);});

  xorN_s xor_filter = xorN_s {};
  allocate(n, &xor_filter);
  populate(hashed_keys, n, &xor_filter);

  size_t fingerprints_bytes = xor_filter.blockLength * 3;
  if (std::is_same<xorN_s, xor16_s>::value)
    fingerprints_bytes *= 2;

  const size_t init_size = dst->size();
  const size_t additional_size = sizeof(uint64_t) + fingerprints_bytes;
  dst->resize(init_size + additional_size, 0);

  uint64_t* seed_ptr = (uint64_t*) &(*dst)[init_size];
  uint8_t* fingerprints_ptr = (uint8_t*) &(*dst)[init_size] + sizeof(uint64_t);

  // save the seed and then the fingerprints
  *seed_ptr = xor_filter.seed;
  std::memcpy(fingerprints_ptr, xor_filter.fingerprints, fingerprints_bytes);

  free(&xor_filter);
  delete[] hashed_keys;
}

template <typename xorN_s>
bool XorFilterPolicy<xorN_s>::KeyMayMatch(const Slice& key, const Slice& filter) const {
  if (filter.size() <= 0)
    return false;

  auto fingerprints_ptr = (uint8_t*) (filter.data() + sizeof(uint64_t)) ;
  uint64_t seed = ((uint64_t *) filter.data())[0];

  uint64_t blocklength = (filter.size() - sizeof(uint64_t)) / 3;

  if (std::is_same<xorN_s, xor16_s>::value)
    blocklength /= 2;

  auto xor_filter = xorN_s {
      seed,
      blocklength,
      nullptr
  };

  return contains(keyHash(key), &xor_filter, fingerprints_ptr);
}

template<>
bool XorFilterPolicy<xor8_s>::contains(
    uint64_t key,
    xor8_s* filter,
    uint8_t* fingerprints_ptr) const
{
  filter->fingerprints = fingerprints_ptr;

  return xor8_contain(key, filter);
}

template<>
void XorFilterPolicy<xor8_s>::allocate(uint32_t size, xor8_s *filter) const
{
  xor8_allocate(size, filter);
}

template<>
void XorFilterPolicy<xor8_s>::free(xor8_s *filter) const
{
  xor8_free(filter);
}

template<>
void XorFilterPolicy<xor8_s>::populate(
    const uint64_t *keys,
    uint32_t size,
    xor8_s *filter) const
{
  xor8_populate(keys, size, filter);
}

template<>
bool XorFilterPolicy<xor16_s>::contains(
    uint64_t key,
    xor16_s* filter,
    uint8_t* fingerprints_ptr) const
{
  filter->fingerprints = (uint16_t *) fingerprints_ptr;

  return xor16_contain(key, filter);
}

template<>
void XorFilterPolicy<xor16_s>::allocate(uint32_t size, xor16_s *filter) const
{
  xor16_allocate(size, filter);
}

template<>
void XorFilterPolicy<xor16_s>::free(xor16_s *filter) const
{
  xor16_free(filter);
}

template<>
void XorFilterPolicy<xor16_s>::populate(const uint64_t *keys, uint32_t size, xor16_s *filter) const
{
  xor16_populate(keys, size, filter);
}


const FilterPolicy* NewXorFilterPolicy(size_t bits_per_key) {
  switch (bits_per_key) {
    case 16:
      return new XorFilterPolicy<xor16_s>();
    default:
      return new XorFilterPolicy<xor8_s>();
  }
}

}  // namespace leveldb