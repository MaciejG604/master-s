//
// Created by mac_g on 22.04.2023.
//

#include <deque>
#include <iostream>
#include <string>

#include "fastfilter_cpp/src/ribbon/ribbon_impl.h"
#include "fastfilter_cpp/src/ribbon/ribbon_math128.h"
#include "fastfilter_cpp/src/ribbon/ribbon_math.h"
//#include "ribbon_config.h"

#ifndef FILTERS_RIBBONFILTERPOLICY_H
#define FILTERS_RIBBONFILTERPOLICY_H

inline uint32_t Lower32of64(uint64_t v) { return static_cast<uint32_t>(v); }
inline uint32_t Upper32of64(uint64_t v) {return static_cast<uint32_t>(v >> 32);}

std::string FinishAlwaysFalse(std::unique_ptr<const char[]>* /*buf*/) {
    // Missing metadata, treated as zero entries
    return std::string(nullptr, 0);
}

std::string FinishAlwaysTrue(std::unique_ptr<const char[]>* /*buf*/) {
    return std::string("\0\0\0\0\0\0", 6);
}

size_t AllocateMaybeRounding(size_t target_len_with_metadata,
                             size_t num_entries,
                             std::unique_ptr<char[]>* buf) {
    // Return value set to a default; overwritten in some cases
    size_t rv = target_len_with_metadata;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    if (aggregate_rounding_balance_ != nullptr) {
      // Do optimize_filters_for_memory, using malloc_usable_size.
      // Approach: try to keep FP rate balance better than or on
      // target (negative aggregate_rounding_balance_). We can then select a
      // lower bound filter size (within reasonable limits) that gets us as
      // close to on target as possible. We request allocation for that filter
      // size and use malloc_usable_size to "round up" to the actual
      // allocation size.

      // Although it can be considered bad practice to use malloc_usable_size
      // to access an object beyond its original size, this approach should be
      // quite general: working for all allocators that properly support
      // malloc_usable_size.

      // Race condition on balance is OK because it can only cause temporary
      // skew in rounding up vs. rounding down, as long as updates are atomic
      // and relative.
      int64_t balance = aggregate_rounding_balance_->load();

      double target_fp_rate =
          EstimatedFpRate(num_entries, target_len_with_metadata);
      double rv_fp_rate = target_fp_rate;

      if (balance < 0) {
        // See formula for BloomFilterPolicy::aggregate_rounding_balance_
        double for_balance_fp_rate =
            -balance / double{0x100000000} + target_fp_rate;

        // To simplify, we just try a few modified smaller sizes. This also
        // caps how much we vary filter size vs. target, to avoid outlier
        // behavior from excessive variance.
        size_t target_len = target_len_with_metadata - kMetadataLen;
        assert(target_len < target_len_with_metadata);  // check underflow
        for (uint64_t maybe_len_rough :
             {uint64_t{3} * target_len / 4, uint64_t{13} * target_len / 16,
              uint64_t{7} * target_len / 8, uint64_t{15} * target_len / 16}) {
          size_t maybe_len_with_metadata =
              RoundDownUsableSpace(maybe_len_rough + kMetadataLen);
          double maybe_fp_rate =
              EstimatedFpRate(num_entries, maybe_len_with_metadata);
          if (maybe_fp_rate <= for_balance_fp_rate) {
            rv = maybe_len_with_metadata;
            rv_fp_rate = maybe_fp_rate;
            break;
          }
        }
      }

      // Filter blocks are loaded into block cache with their block trailer.
      // We need to make sure that's accounted for in choosing a
      // fragmentation-friendly size.
      const size_t kExtraPadding = BlockBasedTable::kBlockTrailerSize;
      size_t requested = rv + kExtraPadding;

      // Allocate and get usable size
      buf->reset(new char[requested]);
      size_t usable = malloc_usable_size(buf->get());

      if (usable - usable / 4 > requested) {
        // Ratio greater than 4/3 is too much for utilizing, if it's
        // not a buggy or mislinked malloc_usable_size implementation.
        // Non-linearity of FP rates with bits/key means rapidly
        // diminishing returns in overall accuracy for additional
        // storage on disk.
        // Nothing to do, except assert that the result is accurate about
        // the usable size. (Assignment never used.)
        assert(((*buf)[usable - 1] = 'x'));
      } else if (usable > requested) {
        rv = RoundDownUsableSpace(usable - kExtraPadding);
        assert(rv <= usable - kExtraPadding);
        rv_fp_rate = EstimatedFpRate(num_entries, rv);
      } else {
        // Too small means bad malloc_usable_size
        assert(usable == requested);
      }
      memset(buf->get(), 0, rv);

      // Update balance
      int64_t diff = static_cast<int64_t>((rv_fp_rate - target_fp_rate) *
                                          double{0x100000000});
      *aggregate_rounding_balance_ += diff;
    } else {
      buf->reset(new char[rv]());
    }
#else
    (void)num_entries;
    buf->reset(new char[rv]());
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return rv;
}

// Implements concept RehasherTypesAndSettings in ribbon_impl.h
struct Standard128RibbonRehasherTypesAndSettings {
    // These are schema-critical. Any change almost certainly changes
    // underlying data.
    static constexpr bool kIsFilter = true;
    static constexpr bool kHomogeneous = false;
    static constexpr bool kFirstCoeffAlwaysOne = true;
    static constexpr bool kUseSmash = false;
    using CoeffRow = ribbon::Unsigned128;
    using Hash = uint64_t;
    using Seed = uint32_t;
    // Changing these doesn't necessarily change underlying data,
    // but might affect supported scalability of those dimensions.
    using Index = uint32_t;
    using ResultRow = uint32_t;
    // Save a conditional in Ribbon queries
    static constexpr bool kAllowZeroStarts = false;
    static constexpr Index kFixedNumColumns = 0;
};

using Standard128RibbonTypesAndSettings =
        ribbon::StandardRehasherAdapter<Standard128RibbonRehasherTypesAndSettings>;

class Standard128RibbonBitsBuilder {
public:
    explicit Standard128RibbonBitsBuilder(double desired_one_in_fp_rate,
                                          bool detect_filter_construct_corruption)
            : desired_one_in_fp_rate_(desired_one_in_fp_rate),
              detect_filter_construct_corruption_(detect_filter_construct_corruption){
        assert(desired_one_in_fp_rate >= 1.0);
    }

    // No Copy allowed
    Standard128RibbonBitsBuilder(const Standard128RibbonBitsBuilder&) = delete;
    void operator=(const Standard128RibbonBitsBuilder&) = delete;

    ~Standard128RibbonBitsBuilder() {}

    static constexpr uint32_t kMetadataLen = 5;

    void AddKey(const uint64_t hashed_key) {
        if (hash_entries_info_.entries.empty() ||
            hashed_key != hash_entries_info_.entries.back()) {
            if (detect_filter_construct_corruption_) {
                hash_entries_info_.xor_checksum ^= hashed_key;
            }
            hash_entries_info_.entries.push_back(hashed_key);
        }
    }

    std::string Finish(std::unique_ptr<const char[]>* buf) {
//        if (hash_entries_info_.entries.size() > kMaxRibbonEntries) {
//            // SOME warning
//            SwapEntriesWith(&bloom_fallback_);
//            assert(hash_entries_info_.entries.empty());
//            return bloom_fallback_.Finish(buf, status);
//        }
        if (hash_entries_info_.entries.size() == 0) {
            return FinishAlwaysFalse(buf);
        }
        uint32_t num_entries =
                static_cast<uint32_t>(hash_entries_info_.entries.size());
        uint32_t num_slots;
        size_t len_with_metadata;

        CalculateSpaceAndSlots(num_entries, &len_with_metadata, &num_slots);

        // Bloom fall-back indicator
//        if (num_slots == 0) {
//            SwapEntriesWith(&bloom_fallback_);
//            assert(hash_entries_info_.entries.empty());
//            return bloom_fallback_.Finish(buf, status);
//        }

        uint32_t entropy = 0;
        if (!hash_entries_info_.entries.empty()) {
            entropy = Lower32of64(hash_entries_info_.entries.front());
        }

        BandingType banding;

        bool success = banding.ResetAndFindSeedToSolve(
                num_slots, hash_entries_info_.entries.begin(),
                hash_entries_info_.entries.end(),
                /*starting seed*/ entropy & 255, /*seed mask*/ 255);
        if (!success) {
            std::cout << "SOME WARNING" << std::endl;
        }

        uint32_t seed = banding.GetOrdinalSeed();
        assert(seed < 256);

        std::unique_ptr<char[]> mutable_buf;
        len_with_metadata =
                AllocateMaybeRounding(len_with_metadata, num_entries, &mutable_buf);

        SolnType soln(mutable_buf.get(), len_with_metadata);
        soln.BackSubstFrom<BandingType>(banding);
        uint32_t num_blocks = soln.GetNumBlocks();
        // This should be guaranteed:
        // num_entries < 2^30
        // => (overhead_factor < 2.0)
        // num_entries * overhead_factor == num_slots < 2^31
        // => (num_blocks = num_slots / 128)
        // num_blocks < 2^24
        assert(num_blocks < 0x1000000U);

        // See BloomFilterPolicy::GetBloomBitsReader re: metadata
        // -2 = Marker for Standard128 Ribbon
        mutable_buf[len_with_metadata - 5] = static_cast<char>(-2);
        // Hash seed
        mutable_buf[len_with_metadata - 4] = static_cast<char>(seed);
        // Number of blocks, in 24 bits
        // (Along with bytes, we can derive other settings)
        mutable_buf[len_with_metadata - 3] = static_cast<char>(num_blocks & 255);
        mutable_buf[len_with_metadata - 2] =
                static_cast<char>((num_blocks >> 8) & 255);
        mutable_buf[len_with_metadata - 1] =
                static_cast<char>((num_blocks >> 16) & 255);


        std::string rv(mutable_buf.get(), len_with_metadata);
        *buf = std::move(mutable_buf);

        return rv;
    }

    // Setting num_slots to 0 means "fall back on Bloom filter."
    // And note this implementation does not support num_entries or num_slots
    // beyond uint32_t; see kMaxRibbonEntries.
    void CalculateSpaceAndSlots(size_t num_entries,
                                size_t* target_len_with_metadata,
                                uint32_t* num_slots) {
        if (num_entries > kMaxRibbonEntries) {
//            // More entries than supported by this Ribbon
//            *num_slots = 0;  // use Bloom
//            *target_len_with_metadata = bloom_fallback_.CalculateSpace(num_entries);
//            return;
        }
        uint32_t entropy = 0;
        if (!hash_entries_info_.entries.empty()) {
            entropy = Upper32of64(hash_entries_info_.entries.front());
        }

        *num_slots = NumEntriesToNumSlots(static_cast<uint32_t>(num_entries));
        *target_len_with_metadata =
                SolnType::GetBytesForOneInFpRate(*num_slots, desired_one_in_fp_rate_,
                        /*rounding*/ entropy) +
                kMetadataLen;

        // Consider possible Bloom fallback for small filters
//        if (*num_slots < 1024) {
//            size_t bloom = bloom_fallback_.CalculateSpace(num_entries);
//            if (bloom < *target_len_with_metadata) {
//                *num_slots = 0;  // use Bloom
//                *target_len_with_metadata = bloom;
//                return;
//            }
//        }
    }

    size_t CalculateSpace(size_t num_entries) {
        if (num_entries == 0) {
            // See FinishAlwaysFalse
            return 0;
        }
        size_t target_len_with_metadata;
        uint32_t num_slots;
        CalculateSpaceAndSlots(num_entries, &target_len_with_metadata, &num_slots);
        (void)num_slots;
        return target_len_with_metadata;
    }

    // This is a somewhat ugly but reasonably fast and reasonably accurate
    // reversal of CalculateSpace.
    size_t ApproximateNumEntries(size_t bytes) {
        size_t len_no_metadata =
                RoundDownUsableSpace(std::max(bytes, size_t{kMetadataLen})) -
                kMetadataLen;

        if (!(desired_one_in_fp_rate_ > 1.0)) {
            // Effectively asking for 100% FP rate, or NaN etc.
            // Note that NaN is neither < 1.0 nor > 1.0
            return kMaxRibbonEntries;
        }

        // Find a slight under-estimate for actual average bits per slot
        double min_real_bits_per_slot;
        if (desired_one_in_fp_rate_ >= 1.0 + std::numeric_limits<uint32_t>::max()) {
            // Max of 32 solution columns (result bits)
            min_real_bits_per_slot = 32.0;
        } else {
            // Account for mix of b and b+1 solution columns being slightly
            // suboptimal vs. ideal log2(1/fp_rate) bits.
            uint32_t rounded = static_cast<uint32_t>(desired_one_in_fp_rate_);
            int upper_bits_per_key = 1 + ribbon::FloorLog2(rounded);
            double fp_rate_for_upper = std::pow(2.0, -upper_bits_per_key);
            double portion_lower =
                    (1.0 / desired_one_in_fp_rate_ - fp_rate_for_upper) /
                    fp_rate_for_upper;
            min_real_bits_per_slot = upper_bits_per_key - portion_lower;
            assert(min_real_bits_per_slot > 0.0);
            assert(min_real_bits_per_slot <= 32.0);
        }

        // An overestimate, but this should only be O(1) slots away from truth.
        double max_slots = len_no_metadata * 8.0 / min_real_bits_per_slot;

        // Let's not bother accounting for overflow to Bloom filter
        // (Includes NaN case)
        if (!(max_slots < ConfigHelper::GetNumSlots(kMaxRibbonEntries))) {
            return kMaxRibbonEntries;
        }

        // Set up for short iteration
        uint32_t slots = static_cast<uint32_t>(max_slots);
        slots = SolnType::RoundUpNumSlots(slots);

        // Assert that we have a valid upper bound on slots
        assert(SolnType::GetBytesForOneInFpRate(
                SolnType::RoundUpNumSlots(slots + 1), desired_one_in_fp_rate_,
                /*rounding*/ 0) > len_no_metadata);

        // Iterate up to a few times to rather precisely account for small effects
        for (int i = 0; slots > 0; ++i) {
            size_t reqd_bytes =
                    SolnType::GetBytesForOneInFpRate(slots, desired_one_in_fp_rate_,
                            /*rounding*/ 0);
            if (reqd_bytes <= len_no_metadata) {
                break;  // done
            }
            if (i >= 2) {
                // should have been enough iterations
                assert(false);
                break;
            }
            slots = SolnType::RoundDownNumSlots(slots - 1);
        }

        uint32_t num_entries = ConfigHelper::GetNumToAdd(slots);

        // Consider possible Bloom fallback for small filters
        if (slots < 1024) {
//            size_t bloom = bloom_fallback_.ApproximateNumEntries(bytes);
//            if (bloom > num_entries) {
//                return bloom;
//            } else {
//                return num_entries;
//            }
            return -1;
        } else {
            return std::min(num_entries, kMaxRibbonEntries);
        }
    }

    double EstimatedFpRate(
      size_t num_entries,
      size_t len_with_metadata) {
        if (num_entries > kMaxRibbonEntries) {
            // More entries than supported by this Ribbon
//            return bloom_fallback_.EstimatedFpRate(num_entries, len_with_metadata);
            return -1.;
        }
        uint32_t num_slots =
                NumEntriesToNumSlots(static_cast<uint32_t>(num_entries));
        SolnType fake_soln(nullptr, len_with_metadata);
        fake_soln.ConfigureForNumSlots(num_slots);
        return fake_soln.ExpectedFpRate();
    }

//    Status MaybePostVerify(const std::string& filter_content) {
//        bool fall_back = (bloom_fallback_.EstimateEntriesAdded() > 0);
//        return fall_back ? bloom_fallback_.MaybePostVerify(filter_content)
//                         : XXPH3FilterBitsBuilder::MaybePostVerify(filter_content);
//    }

protected:
    size_t RoundDownUsableSpace(size_t available_size) {
        size_t rv = available_size - kMetadataLen;

        // round down to multiple of 16 (segment size)
        rv &= ~size_t{15};

        return rv + kMetadataLen;
    }

private:
    using TS = Standard128RibbonTypesAndSettings;
    using SolnType = ribbon::SerializableInterleavedSolution<TS>;
    using BandingType = ribbon::StandardBanding<TS>;
    using ConfigHelper = ribbon::BandingConfigHelper1TS<ribbon::kOneIn20, TS>;

    static uint32_t NumEntriesToNumSlots(uint32_t num_entries) {
        uint32_t num_slots1 = ConfigHelper::GetNumSlots(num_entries);
        return SolnType::RoundUpNumSlots(num_slots1);
    }

    // Approximate num_entries to ensure number of bytes fits in 32 bits, which
    // is not an inherent limitation but does ensure somewhat graceful Bloom
    // fallback for crazy high number of entries, since the Bloom implementation
    // does not support number of bytes bigger than fits in 32 bits. This is
    // within an order of magnitude of implementation limit on num_slots
    // fitting in 32 bits, and even closer for num_blocks fitting in 24 bits
    // (for filter metadata).
    static constexpr uint32_t kMaxRibbonEntries = 950000000;  // ~ 1 billion

    // A desired value for 1/fp_rate. For example, 100 -> 1% fp rate.
    double desired_one_in_fp_rate_;

    bool detect_filter_construct_corruption_;

    struct HashEntriesInfo {
        // A deque avoids unnecessary copying of already-saved values
        // and has near-minimal peak memory use.
        std::deque<uint64_t> entries;

        // If cache_res_mgr_ != nullptr,
        // it manages cache charge for buckets of hash entries in (new) Bloom
        // or Ribbon Filter construction.
        // Otherwise, it is empty.
//        std::deque<std::unique_ptr<CacheReservationManager::CacheReservationHandle>>
//                cache_res_bucket_handles;

        // If detect_filter_construct_corruption_ == true,
        // it records the xor checksum of hash entries.
        // Otherwise, it is 0.
        uint64_t xor_checksum = 0;

        void Swap(HashEntriesInfo* other) {
//            assert(other != nullptr);
//            std::swap(entries, other->entries);
//            std::swap(cache_res_bucket_handles, other->cache_res_bucket_handles);
//            std::swap(xor_checksum, other->xor_checksum);
        }

        void Reset() {
//            entries.clear();
//            cache_res_bucket_handles.clear();
//            xor_checksum = 0;
        }
    };

    HashEntriesInfo hash_entries_info_;
};

// for the linker, at least with DEBUG_LEVEL=2
constexpr uint32_t Standard128RibbonBitsBuilder::kMaxRibbonEntries;

class Standard128RibbonBitsReader {
public:
    Standard128RibbonBitsReader(const char* data, size_t len_bytes,
                                uint32_t num_blocks, uint32_t seed)
            : soln_(const_cast<char*>(data), len_bytes) {
        soln_.ConfigureForNumBlocks(num_blocks);
        hasher_.SetOrdinalSeed(seed);
    }

    // No Copy allowed
    Standard128RibbonBitsReader(const Standard128RibbonBitsReader&) = delete;
    void operator=(const Standard128RibbonBitsReader&) = delete;

    ~Standard128RibbonBitsReader() {}

    bool HashMayMatch(const uint64_t h) {
        return soln_.FilterQuery(h, hasher_);
    }

private:
    using TS = Standard128RibbonTypesAndSettings;
    ribbon::SerializableInterleavedSolution<TS> soln_;
    ribbon::StandardHasher<TS> hasher_;
};

#endif //FILTERS_RIBBONFILTERPOLICY_H
