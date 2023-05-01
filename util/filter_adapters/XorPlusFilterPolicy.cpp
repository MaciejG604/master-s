//
// Created by Maciej Gajek on 30/03/2023.
//

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "fastfilter_cpp/src/xorfilter/xorfilter_plus.h"
#include "util/MurmurHash3.h"

namespace {
 void serialize_rank9(xorfilter_plus::Rank9* rank, std::string* dst) {
    auto initial_size = dst->size();

    auto additional_size = rank->bitsArraySize * sizeof(uint64_t) +
    rank->countsArraySize * sizeof(uint64_t);

    dst->resize(initial_size + additional_size, 0);

    auto bits_array_size_ptr = (uint64_t*) &dst->data()[initial_size];
    auto counts_array_size_ptr = bits_array_size_ptr + 1;
    auto bits_array_ptr = counts_array_size_ptr + 1;
    auto counts_array_ptr = bits_array_ptr + rank->bitsArraySize;

    *bits_array_size_ptr = rank->bitsArraySize;
    *counts_array_size_ptr = rank->countsArraySize;

    std::memcpy(bits_array_ptr, rank->bits, rank->bitsArraySize * sizeof(uint64_t));
    std::memcpy(counts_array_ptr, rank->counts, rank->countsArraySize * sizeof(uint64_t));
 }

 xorfilter_plus::Rank9 deserialize_rank9(void* data) {
    auto bits_array_size_ptr = ((uint64_t*) data);
    auto counts_array_size_ptr = bits_array_size_ptr + 1;
    auto bits_array_ptr = counts_array_size_ptr + 1;
    auto counts_array_ptr = bits_array_ptr + *bits_array_size_ptr;

    auto rank = xorfilter_plus::Rank9 {
        bits_array_ptr,
        *bits_array_size_ptr,
        counts_array_ptr,
        *counts_array_size_ptr,
    };

    return std::move(rank);
 }
}

namespace leveldb {

template <typename fingerprint_t>
class XorPlusFilterPolicy : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorPlusFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    auto* hashed_keys = new uint64_t[n];
    std::transform(keys, &keys[n], hashed_keys, [](const Slice& x){return keyHash(x);});

    auto xor_filter = xorfilter_plus::XorFilterPlus<uint64_t, fingerprint_t>(n);

    xor_filter.AddAll(hashed_keys, 0, n);

    const size_t fingerprint_size = xor_filter.totalSizeInBytes * sizeof(fingerprint_t) -
                                     xor_filter.rank->getBitCount() / 8;

    const size_t init_size = dst->size();
    const size_t additional_size = sizeof(xorfilter_plus::XorFilterPlus<uint64_t, fingerprint_t>) +
       sizeof(hashing::SimpleMixSplit) +
       sizeof(size_t) +
       fingerprint_size;
    dst->resize(init_size + additional_size, 0);

    auto filter_ptr = (xorfilter_plus::XorFilterPlus<uint64_t, fingerprint_t>*) &dst->data()[init_size];
    auto hash_ptr = (hashing::SimpleMixSplit*) (filter_ptr + 1);
    auto fingerprints_size_ptr = (size_t*) hash_ptr + 1;
    auto fingerprints_ptr = (fingerprint_t*) (fingerprints_size_ptr + 1);

    *filter_ptr = xor_filter;
    *hash_ptr = *xor_filter.hasher;
    *fingerprints_size_ptr = fingerprint_size;
    std::memcpy(fingerprints_ptr, xor_filter.fingerprints, fingerprint_size);

    serialize_rank9(xor_filter.rank, dst);

    delete[] hashed_keys;
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.empty()) return false;

    auto filter_ptr =
        (xorfilter_plus::XorFilterPlus<uint64_t, fingerprint_t>*) filter.data();
    auto hash_ptr = (hashing::SimpleMixSplit*)(filter_ptr + 1);
    auto fingerprints_size_ptr = (size_t*)(hash_ptr + 1);
    auto fingerprints_ptr = (fingerprint_t*)(fingerprints_size_ptr + 1);

    filter_ptr->hasher = hash_ptr;
    filter_ptr->fingerprints = fingerprints_ptr;

    auto rank =
        deserialize_rank9((void*)(fingerprints_ptr + *fingerprints_size_ptr / sizeof(fingerprint_t)));
    filter_ptr->rank = &rank;

    auto res = filter_ptr->Contain(keyHash(key)) == xorfilter_plus::Status::Ok;

    rank.bits = nullptr;
    rank.counts = nullptr;

    return res;
  }
};

const FilterPolicy* NewXorPlusFilterPolicy(size_t bits_per_key) {
  switch (bits_per_key) {
    case 16:
      return new XorPlusFilterPolicy<uint16_t>();
    default:
      return new XorPlusFilterPolicy<uint8_t>();
  }
}

}  // namespace leveldb