#include <climits>
#include <iomanip>
#include <map>
#include <stdexcept>
#include <vector>
#include <set>
#include <stdio.h>

#include "fastfilter_cpp/src/ribbon/ribbon_impl.h"

using namespace std;

template <typename CoeffType, bool kHomog, uint32_t kNumColumns, bool kSmash = false>
struct RibbonTS {
  static constexpr bool kIsFilter = true;
  static constexpr bool kHomogeneous = kHomog;
  static constexpr bool kFirstCoeffAlwaysOne = true;
  static constexpr bool kUseSmash = kSmash;
  using CoeffRow = CoeffType;
  using Hash = uint64_t;
  using Key = uint64_t;
  using Seed = uint32_t;
  using Index = size_t;
  using ResultRow = uint32_t;
  static constexpr bool kAllowZeroStarts = false;
  static constexpr uint32_t kFixedNumColumns = kNumColumns;

  static Hash HashFn(const Hash& input, Seed raw_seed) {
    // return input;
    uint64_t h = input + raw_seed;
    h ^= h >> 33;
    h *= UINT64_C(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h *= UINT64_C(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    return h;
  }
};

template <typename CoeffType, uint32_t kNumColumns, uint32_t kMinPctOverhead, uint32_t kMilliBitsPerKey = 7700>
struct BalancedRibbonFilter {
  using TS = RibbonTS<CoeffType, /*kHomog*/ false, kNumColumns>;
  IMPORT_RIBBON_IMPL_TYPES(TS);
  static constexpr uint32_t kBitsPerVshard = 8;
  using BalancedBanding = ribbon::BalancedBanding<TS, kBitsPerVshard>;
  using BalancedHasher = ribbon::BalancedHasher<TS, kBitsPerVshard>;

  uint32_t log2_vshards;
  size_t num_slots;

  size_t bytes;
  unique_ptr<char[]> ptr;
  InterleavedSoln soln;

  size_t meta_bytes;
  unique_ptr<char[]> meta_ptr;
  BalancedHasher hasher;
 public:
  static constexpr double kFractionalCols =
      kNumColumns == 0 ? kMilliBitsPerKey / 1000.0 : kNumColumns;

  static double GetNumSlots(size_t add_count, uint32_t log2_vshards) {
    size_t add_per_vshard = add_count >> log2_vshards;

    double overhead;
    if (sizeof(CoeffType) == 8) {
      overhead = 0.0000055 * add_per_vshard; // FIXME?
    } else if (sizeof(CoeffType) == 4) {
      overhead = 0.00005 * add_per_vshard;
    } else if (sizeof(CoeffType) == 2) {
      overhead = 0.00010 * add_per_vshard; // FIXME?
    } else {
      assert(sizeof(CoeffType) == 16);
      overhead = 0.0000013 * add_per_vshard;
    }
    overhead = std::max(overhead, 0.01 * kMinPctOverhead);
    return InterleavedSoln::RoundUpNumSlots((size_t)(add_count + overhead * add_count + add_per_vshard / 5));
  }

  BalancedRibbonFilter(size_t add_count)
      : log2_vshards((uint32_t)ribbon::FloorLog2((add_count + add_count / 3 + add_count / 5) / (128 * sizeof(CoeffType)))),
        num_slots(GetNumSlots(add_count, log2_vshards)),
        bytes(static_cast<size_t>((num_slots * kFractionalCols + 7) / 8)),
        ptr(new char[bytes]),
        soln(ptr.get(), bytes),
        meta_bytes(BalancedHasher(log2_vshards, nullptr).GetMetadataLength()),
        meta_ptr(new char[meta_bytes]),
        hasher(log2_vshards, meta_ptr.get()) {}

  BalancedRibbonFilter(uint32_t log2_vshards, size_t num_slots, char* bytes_ptr, size_t bytes_len, char* meta_bytes_ptr, size_t meta_len)
      : log2_vshards(log2_vshards),
        num_slots(num_slots),
        bytes(bytes_len),
        ptr(bytes_ptr),
        soln(ptr.get(), bytes_len),
        meta_bytes(meta_len),
        meta_ptr(meta_bytes_ptr),
        hasher(log2_vshards, meta_ptr.get()) {}

  void AddAll(const vector<uint64_t>& keys, const size_t start, const size_t end) {
    BalancedBanding b(log2_vshards);
    b.BalancerAddRange(keys.begin() + start, keys.begin() + end);
    if (!b.Balance(num_slots)) {
      fprintf(stderr, "Failed!\n");
      return;
    }
    soln.BackSubstFrom(b);
    memcpy(meta_ptr.get(), b.GetMetadata(), b.GetMetadataLength());
  }
  bool Contain(uint64_t key) const {
    return soln.FilterQuery(key, hasher);
  }
  size_t SizeInBytes() const {
    return bytes + meta_bytes;
  }
};