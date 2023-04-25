////
//// Created by Maciej Gajek on 03/04/2023.
////
//
//#include "leveldb/filter_policy.h"
//#include "leveldb/slice.h"
//
//#include "util/MurmurHash3.h"
//
//#ifndef XOR_MAX_ITERATIONS
//#define XOR_MAX_ITERATIONS 100 // probabillity of success should always be > 0.5 so 100 iterations is highly unlikely
//#endif
//
//namespace leveldb {
//
//static inline uint64_t xor_murmur64(uint64_t h) {
//  h ^= h >> 33;
//  h *= UINT64_C(0xff51afd7ed558ccd);
//  h ^= h >> 33;
//  h *= UINT64_C(0xc4ceb9fe1a85ec53);
//  h ^= h >> 33;
//  return h;
//}
//
//static inline uint64_t xor_mix_split(uint64_t key, uint64_t seed) {
//  return xor_murmur64(key + seed);
//}
//
//static inline uint64_t xor_rotl64(uint64_t n, unsigned int c) {
//  return (n << (c & 63)) | (n >> ((-c) & 63));
//}
//
//static inline uint32_t xor_reduce(uint32_t hash, uint32_t n) {
//  // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
//  return (uint32_t)(((uint64_t)hash * n) >> 32);
//}
//
//static inline uint64_t xor_fingerprint(uint64_t hash) {
//  return hash ^ (hash >> 32);
//}
//
//static inline uint64_t xor_rng_splitmix64(uint64_t *seed) {
//  uint64_t z = (*seed += UINT64_C(0x9E3779B97F4A7C15));
//  z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
//  z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
//  return z ^ (z >> 31);
//}
//
//typedef struct xor8_s {
//  uint64_t seed;
//  uint64_t blockLength;
//  uint8_t
//      *fingerprints; // after xor8_allocate, will point to 3*blockLength values
//} xor8_t;
//
//static inline void xor8_free(xor8_t *filter) {
//  free(filter->fingerprints);
//  filter->fingerprints = NULL;
//  filter->blockLength = 0;
//}
//
//// Report if the key is in the set, with false positive rate.
//static inline bool xor8_contain(uint64_t key, const xor8_t *filter) {
//  uint64_t hash = xor_mix_split(key, filter->seed);
//  uint8_t f = 0xF & xor_fingerprint(hash);
//  uint32_t r0 = 0xF & (uint32_t)hash;
//  uint32_t r1 = 0xF & (uint32_t)xor_rotl64(hash, 21);
//  uint32_t r2 = 0xF & (uint32_t)xor_rotl64(hash, 42);
//  uint32_t h0 = xor_reduce(r0, filter->blockLength);
//  uint32_t h1 = xor_reduce(r1, filter->blockLength) + filter->blockLength;
//  uint32_t h2 = xor_reduce(r2, filter->blockLength) + 2 * filter->blockLength;
//  return f == (filter->fingerprints[h0] ^ filter->fingerprints[h1] ^
//               filter->fingerprints[h2]);
//}
//
//static inline bool xor8_allocate(uint32_t size, xor8_t *filter) {
//  size_t capacity = 32 + 1.23 * size;
//  capacity = capacity / 3 * 3;
//  filter->fingerprints = (uint8_t *)malloc(capacity * sizeof(uint8_t));
//  if (filter->fingerprints != NULL) {
//    filter->blockLength = capacity / 3;
//    return true;
//  } else {
//    return false;
//  }
//}
//
//static inline size_t xor8_size_in_bytes(const xor8_t *filter) {
//  return 3 * filter->blockLength * sizeof(uint8_t) + sizeof(xor8_t);
//}
//
//struct xor_xorset_s {
//  uint64_t xormask;
//  uint32_t count;
//};
//
//typedef struct xor_xorset_s xor_xorset_t;
//
//struct xor_hashes_s {
//  uint64_t h;
//  uint32_t h0;
//  uint32_t h1;
//  uint32_t h2;
//};
//
//typedef struct xor_hashes_s xor_hashes_t;
//
//static inline xor_hashes_t xor8_get_h0_h1_h2(uint64_t k, const xor8_t *filter) {
//  uint64_t hash = xor_mix_split(k, filter->seed);
//  xor_hashes_t answer;
//  answer.h = hash;
//  uint32_t r0 = 0xF &(uint32_t)hash;
//  uint32_t r1 = 0xF &(uint32_t)xor_rotl64(hash, 21);
//  uint32_t r2 = 0xF &(uint32_t)xor_rotl64(hash, 42);
//
//  answer.h0 = xor_reduce(r0, filter->blockLength);
//  answer.h1 = xor_reduce(r1, filter->blockLength);
//  answer.h2 = xor_reduce(r2, filter->blockLength);
//  return answer;
//}
//
//struct xor_keyindex_s {
//  uint64_t hash;
//  uint32_t index;
//};
//
//typedef struct xor_keyindex_s xor_keyindex_t;
//
//static inline uint32_t xor8_get_h0(uint64_t hash, const xor8_t *filter) {
//  uint32_t r0 = (uint32_t)hash;
//  return xor_reduce(r0, filter->blockLength);
//}
//static inline uint32_t xor8_get_h1(uint64_t hash, const xor8_t *filter) {
//  uint32_t r1 = (uint32_t)xor_rotl64(hash, 21);
//  return xor_reduce(r1, filter->blockLength);
//}
//static inline uint32_t xor8_get_h2(uint64_t hash, const xor8_t *filter) {
//  uint32_t r2 = (uint32_t)xor_rotl64(hash, 42);
//  return xor_reduce(r2, filter->blockLength);
//}
//
//static inline bool xor8_populate(const uint64_t *keys, uint32_t size, xor8_t *filter) {
//  if(size == 0) { return false; }
//  uint64_t rng_counter = 1;
//  filter->seed = xor_rng_splitmix64(&rng_counter);
//  size_t arrayLength = filter->blockLength * 3; // size of the backing array
//  size_t blockLength = filter->blockLength;
//
//  xor_xorset_t *sets =
//      (xor_xorset_t *)malloc(arrayLength * sizeof(xor_xorset_t));
//
//  xor_keyindex_t *Q =
//      (xor_keyindex_t *)malloc(arrayLength * sizeof(xor_keyindex_t));
//
//  xor_keyindex_t *stack =
//      (xor_keyindex_t *)malloc(size * sizeof(xor_keyindex_t));
//
//  if ((sets == NULL) || (Q == NULL) || (stack == NULL)) {
//    free(sets);
//    free(Q);
//    free(stack);
//    return false;
//  }
//  xor_xorset_t *sets0 = sets;
//  xor_xorset_t *sets1 = sets + blockLength;
//  xor_xorset_t *sets2 = sets + 2 * blockLength;
//  xor_keyindex_t *Q0 = Q;
//  xor_keyindex_t *Q1 = Q + blockLength;
//  xor_keyindex_t *Q2 = Q + 2 * blockLength;
//
//  int iterations = 0;
//
//  while (true) {
//    iterations ++;
//    if(iterations > XOR_MAX_ITERATIONS) {
//      // The probability of this happening is lower than the
//      // the cosmic-ray probability (i.e., a cosmic ray corrupts your system),
//      // but if it happens, we just fill the fingerprint with ones which
//      // will flag all possible keys as 'possible', ensuring a correct result.
//      memset(filter->fingerprints, ~0, 3 * filter->blockLength);
//      free(sets);
//      free(Q);
//      free(stack);
//      return false;
//    }
//
//    memset(sets, 0, sizeof(xor_xorset_t) * arrayLength);
//    for (size_t i = 0; i < size; i++) {
//      uint64_t key = keys[i];
//      xor_hashes_t hs = xor8_get_h0_h1_h2(key, filter);
//      sets0[hs.h0].xormask ^= hs.h;
//      sets0[hs.h0].count++;
//      sets1[hs.h1].xormask ^= hs.h;
//      sets1[hs.h1].count++;
//      sets2[hs.h2].xormask ^= hs.h;
//      sets2[hs.h2].count++;
//    }
//    // todo: the flush should be sync with the detection that follows
//    // scan for values with a count of one
//    size_t Q0size = 0, Q1size = 0, Q2size = 0;
//    for (size_t i = 0; i < filter->blockLength; i++) {
//      if (sets0[i].count == 1) {
//        Q0[Q0size].index = i;
//        Q0[Q0size].hash = sets0[i].xormask;
//        Q0size++;
//      }
//    }
//
//    for (size_t i = 0; i < filter->blockLength; i++) {
//      if (sets1[i].count == 1) {
//        Q1[Q1size].index = i;
//        Q1[Q1size].hash = sets1[i].xormask;
//        Q1size++;
//      }
//    }
//    for (size_t i = 0; i < filter->blockLength; i++) {
//      if (sets2[i].count == 1) {
//        Q2[Q2size].index = i;
//        Q2[Q2size].hash = sets2[i].xormask;
//        Q2size++;
//      }
//    }
//
//    size_t stack_size = 0;
//    while (Q0size + Q1size + Q2size > 0) {
//      while (Q0size > 0) {
//        xor_keyindex_t keyindex = Q0[--Q0size];
//        size_t index = keyindex.index;
//        if (sets0[index].count == 0)
//          continue; // not actually possible after the initial scan.
//        //sets0[index].count = 0;
//        uint64_t hash = keyindex.hash;
//        uint32_t h1 = xor8_get_h1(hash, filter);
//        uint32_t h2 = xor8_get_h2(hash, filter);
//
//        stack[stack_size] = keyindex;
//        stack_size++;
//        sets1[h1].xormask ^= hash;
//        sets1[h1].count--;
//        if (sets1[h1].count == 1) {
//          Q1[Q1size].index = h1;
//          Q1[Q1size].hash = sets1[h1].xormask;
//          Q1size++;
//        }
//        sets2[h2].xormask ^= hash;
//        sets2[h2].count--;
//        if (sets2[h2].count == 1) {
//          Q2[Q2size].index = h2;
//          Q2[Q2size].hash = sets2[h2].xormask;
//          Q2size++;
//        }
//      }
//      while (Q1size > 0) {
//        xor_keyindex_t keyindex = Q1[--Q1size];
//        size_t index = keyindex.index;
//        if (sets1[index].count == 0)
//          continue;
//        //sets1[index].count = 0;
//        uint64_t hash = keyindex.hash;
//        uint32_t h0 = xor8_get_h0(hash, filter);
//        uint32_t h2 = xor8_get_h2(hash, filter);
//        keyindex.index += blockLength;
//        stack[stack_size] = keyindex;
//        stack_size++;
//        sets0[h0].xormask ^= hash;
//        sets0[h0].count--;
//        if (sets0[h0].count == 1) {
//          Q0[Q0size].index = h0;
//          Q0[Q0size].hash = sets0[h0].xormask;
//          Q0size++;
//        }
//        sets2[h2].xormask ^= hash;
//        sets2[h2].count--;
//        if (sets2[h2].count == 1) {
//          Q2[Q2size].index = h2;
//          Q2[Q2size].hash = sets2[h2].xormask;
//          Q2size++;
//        }
//      }
//      while (Q2size > 0) {
//        xor_keyindex_t keyindex = Q2[--Q2size];
//        size_t index = keyindex.index;
//        if (sets2[index].count == 0)
//          continue;
//
//        //sets2[index].count = 0;
//        uint64_t hash = keyindex.hash;
//
//        uint32_t h0 = xor8_get_h0(hash, filter);
//        uint32_t h1 = xor8_get_h1(hash, filter);
//        keyindex.index += 2 * blockLength;
//
//        stack[stack_size] = keyindex;
//        stack_size++;
//        sets0[h0].xormask ^= hash;
//        sets0[h0].count--;
//        if (sets0[h0].count == 1) {
//          Q0[Q0size].index = h0;
//          Q0[Q0size].hash = sets0[h0].xormask;
//          Q0size++;
//        }
//        sets1[h1].xormask ^= hash;
//        sets1[h1].count--;
//        if (sets1[h1].count == 1) {
//          Q1[Q1size].index = h1;
//          Q1[Q1size].hash = sets1[h1].xormask;
//          Q1size++;
//        }
//
//      }
//    }
//    if (stack_size == size) {
//      // success
//      break;
//    }
//
//    filter->seed = xor_rng_splitmix64(&rng_counter);
//  }
//  uint8_t * fingerprints0 = filter->fingerprints;
//  uint8_t * fingerprints1 = filter->fingerprints + blockLength;
//  uint8_t * fingerprints2 = filter->fingerprints + 2 * blockLength;
//
//  size_t stack_size = size;
//  while (stack_size > 0) {
//    xor_keyindex_t ki = stack[--stack_size];
//    uint64_t val = xor_fingerprint(ki.hash);
//    if(ki.index < blockLength) {
//      val ^= fingerprints1[xor8_get_h1(ki.hash,filter)] ^ fingerprints2[xor8_get_h2(ki.hash,filter)];
//    } else if(ki.index < 2 * blockLength) {
//      val ^= fingerprints0[xor8_get_h0(ki.hash,filter)] ^ fingerprints2[xor8_get_h2(ki.hash,filter)];
//    } else {
//      val ^= fingerprints0[xor8_get_h0(ki.hash,filter)] ^ fingerprints1[xor8_get_h1(ki.hash,filter)];
//    }
//    filter->fingerprints[ki.index] = val;
//  }
//
//  free(sets);
//  free(Q);
//  free(stack);
//  return true;
//}
//
//class CompressedXorFilterPolicy : public FilterPolicy {
// public:
//  size_t bits_per_key_;
//
//  explicit CompressedXorFilterPolicy(size_t bits_per_key) : bits_per_key_(bits_per_key) {}
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
//  const char* Name() const override { return "CompressedXorFilterPolicy"; }
//
//  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const override {
//    auto* hashed_keys = new uint64_t[n];
//    std::transform(keys, &keys[n], hashed_keys, [](const Slice& x){return keyHash(x);});
//
//    xor8_t xor_filter = xor8_t {};
//    xor8_allocate(n, &xor_filter);
//    xor8_populate(hashed_keys, n, &xor_filter);
//
//    const size_t fingerprints_bytes = xor_filter.blockLength * 3;
//
//    const size_t init_size = dst->size();
//    const size_t additional_size = sizeof(uint64_t) + fingerprints_bytes;
//    dst->resize(init_size + additional_size, 0);
//
//    uint64_t* seed_ptr = (uint64_t*) &(*dst)[init_size];
//    uint8_t* fingerprints_ptr = (uint8_t*) &(*dst)[init_size] + sizeof(uint64_t);
//
//    // save the seed and then the fingerprints
//    *seed_ptr = xor_filter.seed;
//    std::memcpy(fingerprints_ptr, xor_filter.fingerprints, fingerprints_bytes);
//
//    xor8_free(&xor_filter);
//    delete[] hashed_keys;
//  }
//
//  bool KeyMayMatch(const leveldb::Slice& key, const leveldb::Slice& filter) const override {
//    if (filter.size() <= 0)
//      return false;
//
//    uint8_t* fingerprints_ptr = ((uint8_t*) filter.data()) + sizeof(uint64_t);
//    uint64_t seed = ((uint64_t *) filter.data())[0];
//
//    xor8_t xor_filter = xor8_t {
//        seed,
//        (filter.size() - sizeof(uint64_t)) / 3,
//        fingerprints_ptr
//    };
//
//    return xor8_contain(keyHash(key), &xor_filter);
//  }
//};
//
//
//const FilterPolicy* NewCompressedXorFilterPolicy(size_t bits_per_key) {
//  return new CompressedXorFilterPolicy(bits_per_key);
//}
//
//}  // namespace leveldb