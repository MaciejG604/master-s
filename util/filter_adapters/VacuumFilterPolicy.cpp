//
// Created by Maciej Gajek on 30/03/2023.
//

#include <iostream>
#include <memory>

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "util/MurmurHash3.h"

#include "Vacuum-Filter/ModifiedCuckooFilter/src/cuckoofilter.h"

constexpr size_t BITS8_PER_KEY = 8;
constexpr size_t BITS12_PER_KEY = 12;
constexpr size_t BITS16_PER_KEY = 16;

namespace leveldb {
class VacuumFilterPolicy8 : public FilterPolicy {
 public:
  
  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    size_t num_keys = n;
    modifiedcuckoofilter::VacuumFilter<uint64_t, BITS8_PER_KEY> filter(num_keys);

    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    auto num_buckets = filter.table_->num_buckets_ +
                       modifiedcuckoofilter::SingleTable<BITS8_PER_KEY>::kPaddingBuckets;

    //TODO persist only values that are needed
    const size_t additional_size = sizeof(modifiedcuckoofilter::VacuumFilter<uint64_t, BITS8_PER_KEY>) +
                                   sizeof(size_t) +
                                   sizeof(modifiedcuckoofilter::SingleTable<BITS8_PER_KEY>::Bucket) * num_buckets;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (modifiedcuckoofilter::VacuumFilter<uint64_t, BITS8_PER_KEY>*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) &filter_persist_ptr[1];
    auto buckets_persist_ptr = (modifiedcuckoofilter::SingleTable<
                                BITS8_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_,
                sizeof(modifiedcuckoofilter::SingleTable<BITS8_PER_KEY>::Bucket) * num_buckets);

    //    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS8_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    //    F filter(num_keys);
    //
    //    for (int i = 0; i < n; ++i) {
    //      filter.Add(keyHash(keys[i]));
    //    }
    //
    //    const size_t init_size = dst->size();
    //
    //    //TODO persist only values that are needed
    //    const size_t additional_size = sizeof(modifiedcuckoofilter::VacuumFilter<uint64_t, BITS8_PER_KEY>) +
    //                                   sizeof(char) * filter.table_->len_;
    //
    //    dst->resize(init_size + additional_size, 0);
    //
    //    auto filter_persist_ptr = (F*) &(*dst)[init_size];
    //    auto tables_persist_ptr = (char*) &filter_persist_ptr[1];
    //
    //    *filter_persist_ptr = filter;
    //    std::memcpy(tables_persist_ptr, filter.table_->buckets_, sizeof(char) * filter.table_->len_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto vacuum_filter_ptr = (modifiedcuckoofilter::VacuumFilter<uint64_t, BITS8_PER_KEY>*) filter.data();
    auto num_buckets_ptr = (size_t*) &vacuum_filter_ptr[1];
    auto buckets_persist_ptr = (modifiedcuckoofilter::SingleTable<
                                BITS8_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    auto vacuum_filter = *vacuum_filter_ptr;

    vacuum_filter.table_ = new modifiedcuckoofilter::SingleTable<BITS8_PER_KEY>();
    vacuum_filter.table_->num_buckets_ = *num_buckets_ptr;
    vacuum_filter.table_->buckets_ = buckets_persist_ptr;

    auto res = vacuum_filter.Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;

    vacuum_filter.table_->buckets_ = nullptr;
    delete vacuum_filter.table_;
    vacuum_filter.table_ = nullptr;

    return res;
    //    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS8_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    //
    //    auto filter_persist_ptr = (F*) filter.data();
    //    auto tables_persist_ptr = (char*) &filter_persist_ptr[1];
    //
    //    filter_persist_ptr->table_->buckets_ = tables_persist_ptr;
    //    filter_persist_ptr->table_->perm_ = modifiedcuckoofilter::PermEncoding();
    //
    //    return filter_persist_ptr->Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;
  }
};

class VacuumFilterPolicy12 : public FilterPolicy {
 public:
  
  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    size_t num_keys = n;
    modifiedcuckoofilter::VacuumFilter<uint64_t, BITS12_PER_KEY> filter(num_keys);
    
    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    auto num_buckets = filter.table_->num_buckets_ +
                       modifiedcuckoofilter::SingleTable<BITS12_PER_KEY>::kPaddingBuckets;

    //TODO persist only values that are needed
    const size_t additional_size = sizeof(modifiedcuckoofilter::VacuumFilter<uint64_t, BITS12_PER_KEY>) +
                                   sizeof(size_t) +
                                   sizeof(modifiedcuckoofilter::SingleTable<BITS12_PER_KEY>::Bucket) * num_buckets;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (modifiedcuckoofilter::VacuumFilter<uint64_t, BITS12_PER_KEY>*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) &filter_persist_ptr[1];
    auto buckets_persist_ptr = (modifiedcuckoofilter::SingleTable<
                                BITS12_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_,
                sizeof(modifiedcuckoofilter::SingleTable<BITS12_PER_KEY>::Bucket) * num_buckets);

    //    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS12_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    //    F filter(num_keys);
    //
    //    for (int i = 0; i < n; ++i) {
    //      filter.Add(keyHash(keys[i]));
    //    }
    //
    //    const size_t init_size = dst->size();
    //
    //    //TODO persist only values that are needed
    //    const size_t additional_size = sizeof(modifiedcuckoofilter::VacuumFilter<uint64_t, BITS12_PER_KEY>) +
    //                                   sizeof(char) * filter.table_->len_;
    //
    //    dst->resize(init_size + additional_size, 0);
    //
    //    auto filter_persist_ptr = (F*) &(*dst)[init_size];
    //    auto tables_persist_ptr = (char*) &filter_persist_ptr[1];
    //
    //    *filter_persist_ptr = filter;
    //    std::memcpy(tables_persist_ptr, filter.table_->buckets_, sizeof(char) * filter.table_->len_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto vacuum_filter_ptr = (modifiedcuckoofilter::VacuumFilter<uint64_t, BITS12_PER_KEY>*) filter.data();
    auto num_buckets_ptr = (size_t*) &vacuum_filter_ptr[1];
    auto buckets_persist_ptr = (modifiedcuckoofilter::SingleTable<
                                BITS12_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    auto vacuum_filter = *vacuum_filter_ptr;

    vacuum_filter.table_ = new modifiedcuckoofilter::SingleTable<BITS12_PER_KEY>();
    vacuum_filter.table_->num_buckets_ = *num_buckets_ptr;
    vacuum_filter.table_->buckets_ = buckets_persist_ptr;

    auto res = vacuum_filter.Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;

    vacuum_filter.table_->buckets_ = nullptr;
    delete vacuum_filter.table_;
    vacuum_filter.table_ = nullptr;

    return res;
    //    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS12_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    //
    //    auto filter_persist_ptr = (F*) filter.data();
    //    auto tables_persist_ptr = (char*) &filter_persist_ptr[1];
    //
    //    filter_persist_ptr->table_->buckets_ = tables_persist_ptr;
    //    filter_persist_ptr->table_->perm_ = modifiedcuckoofilter::PermEncoding();
    //
    //    return filter_persist_ptr->Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;
  }
};

class VacuumFilterPolicy16 : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    size_t num_keys = n;
    modifiedcuckoofilter::VacuumFilter<uint64_t, BITS16_PER_KEY> filter(num_keys);
    
    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    auto num_buckets = filter.table_->num_buckets_ +
                       modifiedcuckoofilter::SingleTable<BITS16_PER_KEY>::kPaddingBuckets;

    //TODO persist only values that are needed
    const size_t additional_size = sizeof(modifiedcuckoofilter::VacuumFilter<uint64_t, BITS16_PER_KEY>) +
                                   sizeof(size_t) +
                                   sizeof(modifiedcuckoofilter::SingleTable<BITS16_PER_KEY>::Bucket) * num_buckets;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (modifiedcuckoofilter::VacuumFilter<uint64_t, BITS16_PER_KEY>*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) &filter_persist_ptr[1];
    auto buckets_persist_ptr = (modifiedcuckoofilter::SingleTable<
                                BITS16_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_,
                sizeof(modifiedcuckoofilter::SingleTable<BITS16_PER_KEY>::Bucket) * num_buckets);

    //    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS16_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    //    F filter(num_keys);
    //
    //    for (int i = 0; i < n; ++i) {
    //      filter.Add(keyHash(keys[i]));
    //    }
    //
    //    const size_t init_size = dst->size();
    //
    //    //TODO persist only values that are needed
    //    const size_t additional_size = sizeof(modifiedcuckoofilter::VacuumFilter<uint64_t, BITS16_PER_KEY>) +
    //                                   sizeof(char) * filter.table_->len_;
    //
    //    dst->resize(init_size + additional_size, 0);
    //
    //    auto filter_persist_ptr = (F*) &(*dst)[init_size];
    //    auto tables_persist_ptr = (char*) &filter_persist_ptr[1];
    //
    //    *filter_persist_ptr = filter;
    //    std::memcpy(tables_persist_ptr, filter.table_->buckets_, sizeof(char) * filter.table_->len_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    auto vacuum_filter_ptr = (modifiedcuckoofilter::VacuumFilter<uint64_t, BITS16_PER_KEY>*) filter.data();
    auto num_buckets_ptr = (size_t*) &vacuum_filter_ptr[1];
    auto buckets_persist_ptr = (modifiedcuckoofilter::SingleTable<
                                BITS16_PER_KEY>::Bucket*) &num_buckets_ptr[1];

    auto vacuum_filter = *vacuum_filter_ptr;

    vacuum_filter.table_ = new modifiedcuckoofilter::SingleTable<BITS16_PER_KEY>();
    vacuum_filter.table_->num_buckets_ = *num_buckets_ptr;
    vacuum_filter.table_->buckets_ = buckets_persist_ptr;

    auto res = vacuum_filter.Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;

    vacuum_filter.table_->buckets_ = nullptr;
    delete vacuum_filter.table_;
    vacuum_filter.table_ = nullptr;

    return res;
    //    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS16_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    //
    //    auto filter_persist_ptr = (F*) filter.data();
    //    auto tables_persist_ptr = (char*) &filter_persist_ptr[1];
    //
    //    filter_persist_ptr->table_->buckets_ = tables_persist_ptr;
    //    filter_persist_ptr->table_->perm_ = modifiedcuckoofilter::PermEncoding();
    //
    //    return filter_persist_ptr->Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;
  }
};

class VacuumFilterPolicy8Packed : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    size_t num_keys = n;

    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS8_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    F filter(num_keys);

    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    const size_t additional_size = sizeof(F) +
                                   sizeof(size_t) +
                                   sizeof(char) * filter.table_->len_;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (F*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) (filter_persist_ptr + 1);
    auto buckets_persist_ptr = (char*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_, sizeof(char) * filter.table_->len_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS8_PER_KEY+1, modifiedcuckoofilter::PackedTable>;

    auto filter_persist_ptr = (F*) filter.data();
    auto num_buckets_ptr = (size_t*) (filter_persist_ptr + 1);
    auto buckets_persist_ptr = (char*) &num_buckets_ptr[1];

    auto vacuum_filter = *filter_persist_ptr;
    vacuum_filter.table_ = new modifiedcuckoofilter::PackedTable<BITS8_PER_KEY+1>();
    vacuum_filter.table_->num_buckets_ = *num_buckets_ptr;
    vacuum_filter.table_->len_ = vacuum_filter.table_->kBytesPerBucket * (*num_buckets_ptr) + 7;
    vacuum_filter.table_->buckets_ = buckets_persist_ptr;
    vacuum_filter.table_->perm_ = modifiedcuckoofilter::PermEncoding();

    auto res = vacuum_filter.Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;

    vacuum_filter.table_->buckets_ = nullptr;
    delete vacuum_filter.table_;
    vacuum_filter.table_ = nullptr;

    return res;
  }
};

class VacuumFilterPolicy12Packed : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "VacuumFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    size_t num_keys = n;
    
    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS12_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    F filter(num_keys);

    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    const size_t additional_size = sizeof(F) +
                                   sizeof(size_t) +
                                   sizeof(char) * filter.table_->len_;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (F*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) (filter_persist_ptr + 1);
    auto buckets_persist_ptr = (char*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_, sizeof(char) * filter.table_->len_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS12_PER_KEY+1, modifiedcuckoofilter::PackedTable>;

    auto filter_persist_ptr = (F*) filter.data();
    auto num_buckets_ptr = (size_t*) (filter_persist_ptr + 1);
    auto buckets_persist_ptr = (char*) &num_buckets_ptr[1];

    auto vacuum_filter = *filter_persist_ptr;
    vacuum_filter.table_ = new modifiedcuckoofilter::PackedTable<BITS12_PER_KEY+1>();
    vacuum_filter.table_->num_buckets_ = *num_buckets_ptr;
    vacuum_filter.table_->len_ = vacuum_filter.table_->kBytesPerBucket * (*num_buckets_ptr) + 7;
    vacuum_filter.table_->buckets_ = buckets_persist_ptr;
    vacuum_filter.table_->perm_ = modifiedcuckoofilter::PermEncoding();

    auto res = vacuum_filter.Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;

    vacuum_filter.table_->buckets_ = nullptr;
    delete vacuum_filter.table_;
    vacuum_filter.table_ = nullptr;

    return res;
  }
};

class VacuumFilterPolicy16Packed : public FilterPolicy {
 public:

  static uint64_t keyHash(const leveldb::Slice& key) {

    uint64_t hash_value = 0;

    MurmurHash3_x86_32((void*) key.data(), key.size(), 1e9 + 9, &hash_value);

    return hash_value;
  }

  const char* Name() const override { return "XorFilterPolicy"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
    size_t num_keys = n;
    
    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS16_PER_KEY+1, modifiedcuckoofilter::PackedTable>;
    F filter(num_keys);

    for (int i = 0; i < n; ++i) {
      filter.Add(keyHash(keys[i]));
    }

    const size_t init_size = dst->size();

    const size_t additional_size = sizeof(F) +
                                   sizeof(size_t) +
                                   sizeof(char) * filter.table_->len_;

    dst->resize(init_size + additional_size, 0);

    auto filter_persist_ptr = (F*) &(*dst)[init_size];
    auto num_buckets_ptr = (size_t*) (filter_persist_ptr + 1);
    auto buckets_persist_ptr = (char*) &num_buckets_ptr[1];

    *filter_persist_ptr = filter;
    *num_buckets_ptr = filter.table_->num_buckets_;
    std::memcpy(buckets_persist_ptr, filter.table_->buckets_, sizeof(char) * filter.table_->len_);
  }

  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
    if (filter.size() <= 0)
      return false;

    using F = modifiedcuckoofilter::VacuumFilter<size_t, BITS16_PER_KEY+1, modifiedcuckoofilter::PackedTable>;

    auto filter_persist_ptr = (F*) filter.data();
    auto num_buckets_ptr = (size_t*) (filter_persist_ptr + 1);
    auto buckets_persist_ptr = (char*) &num_buckets_ptr[1];

    auto vacuum_filter = *filter_persist_ptr;
    vacuum_filter.table_ = new modifiedcuckoofilter::PackedTable<BITS16_PER_KEY+1>();
    vacuum_filter.table_->num_buckets_ = *num_buckets_ptr;
    vacuum_filter.table_->len_ = vacuum_filter.table_->kBytesPerBucket * (*num_buckets_ptr) + 7;
    vacuum_filter.table_->buckets_ = buckets_persist_ptr;
    vacuum_filter.table_->perm_ = modifiedcuckoofilter::PermEncoding();

    auto res = vacuum_filter.Contain(keyHash(key)) == modifiedcuckoofilter::Status::Ok;

    vacuum_filter.table_->buckets_ = nullptr;
    delete vacuum_filter.table_;
    vacuum_filter.table_ = nullptr;

    return res;
  }
};

const FilterPolicy* NewVacuumFilterPolicy(size_t bits_per_key, bool packed) {
  if (packed) {
    switch (bits_per_key) {
      case BITS12_PER_KEY:
        return new VacuumFilterPolicy12Packed();
      case BITS16_PER_KEY:
        return new VacuumFilterPolicy16Packed();
      default:
        return new VacuumFilterPolicy8Packed();
    }
  }

  switch (bits_per_key) {
    case BITS12_PER_KEY:
      return new VacuumFilterPolicy12();
    case BITS16_PER_KEY:
      return new VacuumFilterPolicy16();
    default:
      return new VacuumFilterPolicy8();
  }
}

}  // namespace leveldb