//
// Created by Maciej Gajek on 30/03/2023.
//

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "cuckoofilter/src/cuckoofilter.h"
#include "cuckoofilter/src/singletable.h"
#include "util/MurmurHash3.h"

namespace leveldb {
constexpr size_t BITS8_PER_KEY = 8;
constexpr size_t BITS12_PER_KEY = 12;
constexpr size_t BITS16_PER_KEY = 16;

class CuckooFilterPolicy8 : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "CuckooFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    //TODO add more bits
    auto filter = cuckoofilter::CuckooFilter<uint64_t, BITS8_PER_KEY>(n);
    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    //TODO persist only values that are needed
    const size_t additional_size = sizeof(cuckoofilter::CuckooFilter<uint64_t, BITS8_PER_KEY>) +
                                   sizeof(size_t) +
                                   sizeof(cuckoofilter::SingleTable<BITS8_PER_KEY>::Bucket) * filter.table_->num_buckets_;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (cuckoofilter::CuckooFilter<uint64_t, BITS8_PER_KEY>*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) &filter_persist_ptr[1];
    auto buckets_persist_ptr = (cuckoofilter::SingleTable<
                                BITS8_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_,
                sizeof(cuckoofilter::SingleTable<BITS8_PER_KEY>::Bucket) * filter.table_->num_buckets_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto cuckoo_filter_ptr = (cuckoofilter::CuckooFilter<uint64_t, BITS8_PER_KEY>*) filter.data();
    auto num_buckets_ptr = (size_t*) &cuckoo_filter_ptr[1];
    auto buckets_persist_ptr = (cuckoofilter::SingleTable<
                                BITS8_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    auto cuckoo_filter = *cuckoo_filter_ptr;

    cuckoo_filter.table_ = new cuckoofilter::SingleTable<BITS8_PER_KEY>();
    cuckoo_filter.table_->num_buckets_ = *num_buckets_ptr;
    cuckoo_filter.table_->buckets_ = buckets_persist_ptr;

    auto res = cuckoo_filter.Contain(keyHash(key)) == cuckoofilter::Status::Ok;

    cuckoo_filter.table_->buckets_ = nullptr;
    delete cuckoo_filter.table_;
    cuckoo_filter.table_ = nullptr;

    return res;
  }

 private:
  size_t bits_per_key_;
};

class CuckooFilterPolicy12 : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "CuckooFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    //TODO add more bits
    auto filter = cuckoofilter::CuckooFilter<uint64_t, BITS12_PER_KEY>(n);
    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }
    
    const size_t init_size = dst->size();

    //TODO persist only values that are needed
    const size_t additional_size = sizeof(cuckoofilter::CuckooFilter<uint64_t, BITS12_PER_KEY>) +
                                   sizeof(size_t) +
                                   sizeof(cuckoofilter::SingleTable<BITS12_PER_KEY>::Bucket) * filter.table_->num_buckets_;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (cuckoofilter::CuckooFilter<uint64_t, BITS12_PER_KEY>*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) &filter_persist_ptr[1];
    auto buckets_persist_ptr = (cuckoofilter::SingleTable<
                                BITS12_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_,
                sizeof(cuckoofilter::SingleTable<BITS12_PER_KEY>::Bucket) * filter.table_->num_buckets_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto cuckoo_filter_ptr = (cuckoofilter::CuckooFilter<uint64_t, BITS12_PER_KEY>*) filter.data();
    auto num_buckets_ptr = (size_t*) &cuckoo_filter_ptr[1];
    auto buckets_persist_ptr = (cuckoofilter::SingleTable<
                                BITS12_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    auto cuckoo_filter = *cuckoo_filter_ptr;

    cuckoo_filter.table_ = new cuckoofilter::SingleTable<BITS12_PER_KEY>();
    cuckoo_filter.table_->num_buckets_ = *num_buckets_ptr;
    cuckoo_filter.table_->buckets_ = buckets_persist_ptr;

    auto res = cuckoo_filter.Contain(keyHash(key)) == cuckoofilter::Status::Ok;

    cuckoo_filter.table_->buckets_ = nullptr;
    delete cuckoo_filter.table_;
    cuckoo_filter.table_ = nullptr;

    return res;
  }

 private:
  size_t bits_per_key_;
};

class CuckooFilterPolicy16 : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "CuckooFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    //TODO add more bits
    auto filter = cuckoofilter::CuckooFilter<uint64_t, BITS16_PER_KEY>(n);
    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }
    
    const size_t init_size = dst->size();
    
    //TODO persist only values that are needed
    const size_t additional_size = sizeof(cuckoofilter::CuckooFilter<uint64_t, BITS16_PER_KEY>) +
                                   sizeof(size_t) +
                                   sizeof(cuckoofilter::SingleTable<BITS16_PER_KEY>::Bucket) * filter.table_->num_buckets_;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (cuckoofilter::CuckooFilter<uint64_t, BITS16_PER_KEY>*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) &filter_persist_ptr[1];
    auto buckets_persist_ptr = (cuckoofilter::SingleTable<
                                BITS16_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_,
                sizeof(cuckoofilter::SingleTable<BITS16_PER_KEY>::Bucket) * filter.table_->num_buckets_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto cuckoo_filter_ptr = (cuckoofilter::CuckooFilter<uint64_t, BITS16_PER_KEY>*) filter.data();
    auto num_buckets_ptr = (size_t*) &cuckoo_filter_ptr[1];
    auto buckets_persist_ptr = (cuckoofilter::SingleTable<
                                BITS16_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    auto cuckoo_filter = *cuckoo_filter_ptr;

    cuckoo_filter.table_ = new cuckoofilter::SingleTable<BITS16_PER_KEY>();
    cuckoo_filter.table_->num_buckets_ = *num_buckets_ptr;
    cuckoo_filter.table_->buckets_ = buckets_persist_ptr;

    auto res = cuckoo_filter.Contain(keyHash(key)) == cuckoofilter::Status::Ok;

    cuckoo_filter.table_->buckets_ = nullptr;
    delete cuckoo_filter.table_;
    cuckoo_filter.table_ = nullptr;

    return res;
  }

 private:
  size_t bits_per_key_;
};

const FilterPolicy* NewCuckooFilterPolicy(size_t bits_per_key) {
  switch (bits_per_key) {
    case BITS12_PER_KEY:
      return new CuckooFilterPolicy12();
    case BITS16_PER_KEY:
      return new CuckooFilterPolicy16();
    default:
      return new CuckooFilterPolicy8();
  }
}

}  // namespace leveldb