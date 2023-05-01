////
//// Created by Maciej Gajek on 30/03/2023.
////
//
//#include "leveldb/filter_policy.h"
//#include "leveldb/slice.h"
//
//#include "morton_filter/morton_sample_configs.h"
//#include "util/MurmurHash3.h"
//
//namespace leveldb {
//
//class MortonFilterPolicy : public FilterPolicy {
// public:
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
//  const char* Name() const override { return "MortonFilterPolicy"; }
//
//  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
//    std::vector<uint64_t> vec(n);
//
//    for (int i = 0; i < n; ++i) {
//      vec[i] = keyHash(keys[i]);
//    }
//
//    std::vector<bool> status(n);
//
//    auto filter = new CompressedCuckoo::Morton3_8((size_t) (2.1 * n) + 128);//(size_t) (n / 0.95) + 64);
//
//    filter->insert_many(vec, status, n);
//
//    auto init_size = dst->size();
//    auto additional_size = sizeof(CompressedCuckoo::Morton3_8*);
//
//    dst->resize(init_size + additional_size);
//
//    auto filter_ptr = (CompressedCuckoo::Morton3_8**) (dst->data() + init_size);
//
//    std::cout <<
//        std::all_of(vec.begin(), vec.end(), [filter](uint64_t key) { return filter->likely_contains(key); })
//        << std::endl;
//
//    *filter_ptr = filter;
//    delete filter;
//  }
//
//  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
//    if (filter.empty()) return false;
//
//    auto morton_filter_ptr = *(CompressedCuckoo::Morton3_8**) filter.data();
//
//    return morton_filter_ptr->likely_contains(keyHash(key));
//  }
//};
//
//const FilterPolicy* NewMortonFilterPolicy(size_t bits_per_key) {
//  switch (bits_per_key) {
////    case BITS12_PER_KEY:
////      return new MortonFilterPolicy12();
////    case BITS16_PER_KEY:
////      return new MortonFilterPolicy16();
//    default:
//      return new MortonFilterPolicy();
//  }
//}
//
//}  // namespace leveldb