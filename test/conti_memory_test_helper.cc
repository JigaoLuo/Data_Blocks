// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include <math.h>
#include <gtest/gtest.h>
#include <vector>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <tuple>
#include <map>
#include <algorithm>
#include <functional>
#include "imlab/util/Random.h"
#include "imlab/continuous_memory/continuous_memory.h"
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::DataBlock;
using imlab::BufferManager;
using imlab::BufferFrame;
using imlab::Continuous_Memory_Segment;
using imlab::PAGE_SIZE;
using imlab::TUPLES_PER_DATABLOCK;
using std::equal_to;
using std::greater;
using std::less;
// ---------------------------------------------------------------------------------------------------
/// Maximal number of Bufferframe in Memory
static constexpr const size_t MAX_PAGES_NUM = 40;

static constexpr const size_t SHIFT_BITS_BYTE1 = 64 - 8;

static constexpr const size_t SHIFT_BITS_BYTE2 = 64 - 16;

static constexpr const size_t SHIFT_BITS_BYTE4 = 64 - 32;

static constexpr const size_t SHIFT_BITS_BYTE8 = 64 - 64;

static std::array<uint32_t, TUPLES_PER_DATABLOCK> INDEX_VECTORS[1];
std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
// ---------------------------------------------------------------------------------------------------
/**
 * Helper function for unary predicate
 * @tparam COLUMN_COUNT
 * @tparam compare_type
 * @tparam Operator
 * @param size
 * @param table_portion
 * @param cm
 * @param op
 */
template <size_t COLUMN_COUNT, CompareType compare_type, size_t shift_bits, class Operator>
void scan_helper_unary_predicate(size_t size,
                                 std::vector<std::vector<uint64_t>>& table_portion,
                                 Continuous_Memory_Segment<COLUMN_COUNT>& cm,
                                 Operator op) {
  Random random;
  for (const auto& table : table_portion) {
      for (size_t i = 0; i < size; ++i) {
      std::vector<Predicate> predicates;
      predicates.reserve(1);
      predicates.emplace_back(0, table[i], compare_type);
      uint64_t compared = table[i];
      uint64_t count_if = std::count_if(table.begin(), table.end(), [compared, op](uint64_t entry) { return op(entry, compared);});
      EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));

      uint64_t random1 = random.Rand() >> shift_bits;
      Predicate predicate_1(0, random1, compare_type);
      predicates[0] = predicate_1;
      count_if = std::count_if(table.begin(), table.end(), [random1, op](uint64_t entry) { return op(entry, random1);});
      EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
  }
}

template <size_t COLUMN_COUNT, CompareType compare_type, size_t shift_bits, class Operator>
void scan_helper_str_unary_predicate(size_t size,
                                     std::vector<std::vector<std::string>>& table_portion,
                                     Continuous_Memory_Segment<COLUMN_COUNT>& cm,
                                     Operator op) {
    Random random;
    const std::vector<std::set<std::string>> str_set_vec = cm.get_dictionaries();
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<Predicate> predicates;
    predicates.reserve(1);
    size_t count = 0;
    predicates.emplace_back(0, "RAIL", compare_type);
//    predicates.emplace_back(0, 0, compare_type);
    for (size_t i = 0; i < 1000000; i++) {
        count = cm.cm_scan(predicates, *index_vectors);
    }
//    for (const auto& table : table_portion) {
//        for (size_t i = 0; i < size; ++i) {
//            std::vector<Predicate> predicates;
//            predicates.reserve(1);
//            std::string compared_str = table[i];
//
//            /// Binary Search
////            auto found = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), compared_str);
////            uint64_t compared = std::distance(str_set_vec[0].begin(), found);
//
//            predicates.emplace_back(0, compared_str, compare_type);
//
//            uint64_t count_if = std::count_if(table.begin(), table.end(), [compared_str, op](std::string entry) { return op(entry, compared_str);});
//            EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
//        }
//    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    std::cout << time_vec << "\n";
    std::cout << count << "\n";
}

template <size_t COLUMN_COUNT, CompareType compare_type, size_t shift_bits>
void scan_helper_binary_predicate(size_t size,
                                 std::vector<std::vector<uint64_t>>& table_portion,
                                 Continuous_Memory_Segment<COLUMN_COUNT>& cm) {
  Random random;
  for (const auto& table : table_portion) {
    for (size_t i = 0; i < size; ++i) {
      std::vector<Predicate> predicates;
      predicates.reserve(1);
      uint64_t compared_l = std::min(table[i], table[size - i - 1]);
      uint64_t compared_r = std::max(table[i], table[size - i - 1]);
      predicates.emplace_back(0, compared_l, compared_r, compare_type);
      uint64_t count_if = std::count_if(table.begin(), table.end(),
          [compared_l, compared_r](uint64_t entry) { return compared_l <= entry && entry <= compared_r;});
      EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));

      uint64_t random1 = random.Rand() >> shift_bits;
      uint64_t random2 = random.Rand() >> shift_bits;
      compared_l = std::min(random1, random2);
      compared_r = std::max(random1, random2);
      Predicate predicate_1(0, compared_l, compared_r, compare_type);
      predicates[0] = predicate_1;
      count_if = std::count_if(table.begin(), table.end(), [compared_l, compared_r](uint64_t entry) { return compared_l <= entry && entry <= compared_r; });
      EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
  }
}

template <size_t COLUMN_COUNT, CompareType compare_type, size_t shift_bits>
void scan_helper_str_binary_predicate(size_t size,
                                      std::vector<std::vector<std::string>>& table_portion,
                                      Continuous_Memory_Segment<COLUMN_COUNT>& cm) {
  Random random;
  const std::vector<std::set<std::string>> str_set_vec = cm.get_dictionaries();
  for (const auto& table : table_portion) {
    for (size_t i = 0; i < size; ++i) {
      std::vector<Predicate> predicates;
      predicates.reserve(1);
      std::string compared_str_l = std::min(table[i], table[size - i - 1]);
      std::string compared_str_r = std::max(table[i], table[size - i - 1]);

      /// Binary Search
//      auto found_l = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), compared_str_l);
//      uint64_t compared_l = std::distance(str_set_vec[0].begin(), found_l);
//
//      auto found_r = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), compared_str_r);
//      uint64_t compared_r = std::distance(str_set_vec[0].begin(), found_r);


      predicates.emplace_back(0, compared_str_l, compared_str_r, compare_type);
      uint64_t count_if = std::count_if(table.begin(), table.end(),
                                        [compared_str_l, compared_str_r](std::string entry) { return compared_str_l <= entry && entry <= compared_str_r;});
      EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
  }
}
// ---------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
