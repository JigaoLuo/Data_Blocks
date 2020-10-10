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
#include "imlab/datablock/DataBlock.h"
#include "imlab/util/Random.h"
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::DataBlock;
using imlab::TUPLES_PER_DATABLOCK;
// ---------------------------------------------------------------------------------------------------
static std::array<uint32_t, TUPLES_PER_DATABLOCK> INDEX_VECTORS[1];
std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
// ---------------------------------------------------------------------------------------------------

/**
 * Helper function for unary predicate
 * @tparam COLUMN_COUNT
 * @tparam CallBack
 * @param size
 * @param table_portion
 * @param db
 * @param compareType
 * @param f
 */
template <size_t COLUMN_COUNT, class CallBack>
void scan_helper_singleCol_uni(
    size_t size,
    const std::vector<std::vector<uint64_t>> &table_portion,
    std::unique_ptr<DataBlock<COLUMN_COUNT>,
                    typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> db,
    CompareType compareType,
    CallBack f) {
  Random random;
  for (const auto &table : table_portion) {
    for (size_t i = 0; i < size; ++i) {
      std::vector<Predicate> predicates;
      predicates.reserve(1);
      predicates.emplace_back(0, table[i], compareType);
      uint64_t compared = table[i];
      uint64_t count_if =
          std::count_if(table.begin(), table.end(),
              [compared, f](uint64_t entry) { return f(compared, entry); });
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));

      uint64_t random1 = random.Rand() >> 56;
      Predicate predicate_1(0, random1, compareType);
      predicates[0] = predicate_1;
      count_if =
          std::count_if(table.begin(), table.end(),
                        [random1, f](uint64_t entry) { return f(random1, entry); });
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
    }
  }
}

template <size_t COLUMN_COUNT, class CallBack>
void scan_helper_str_singleCol_uni(size_t size,
                                   const std::vector<std::vector<std::string>> &table_portion,
                                   const std::vector<std::set<std::string>> &str_set_vec,
                                   std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> db,
                                   CompareType compareType,
                                   CallBack f) {
  Random random;
  for (const auto &table : table_portion) {
    for (size_t i = 0; i < size; ++i) {
      std::vector<Predicate> predicates;
      predicates.reserve(1);
      std::string compared_str = table[i];

      /// Binary Search
//      auto found = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), compared_str);
//      uint64_t compared = std::distance(str_set_vec[0].begin(), found);
//      auto compared = db->decode_string(compared_str, 0);

      predicates.emplace_back(0, compared_str, compareType);
      uint64_t count_if =
          std::count_if(table.begin(), table.end(),
                        [compared_str, f](std::string entry) { return f(compared_str, entry); });
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
    }
  }
}

/**
 * Helper function for binary predicate
 * @tparam COLUMN_COUNT
 * @tparam CallBack
 * @param size
 * @param table_portion
 * @param db
 * @param compareType
 * @param f
 */
template <size_t COLUMN_COUNT>
void scan_helper_singleCol_bin(
    size_t size, std::vector<std::vector<uint64_t>> &table_portion,
    std::unique_ptr<DataBlock<COLUMN_COUNT>,
                    typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> db) {
  Random random;
  for (const auto &table : table_portion) {
    for (size_t i = 0; i < size; ++i) {
      std::vector<Predicate> predicates;
      predicates.reserve(1);
      uint64_t compared_l = std::min(table[i], table[size - i - 1]);
      uint64_t compared_r = std::max(table[i], table[size - i - 1]);
      predicates.emplace_back(0, compared_l, compared_r, CompareType::BETWEEN);
      uint64_t count_if = std::count_if(
          table.begin(), table.end(), [compared_l, compared_r](uint64_t entry) {
            return compared_l <= entry && entry <= compared_r;
          });
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));

      uint64_t random1 = random.Rand() >> 56;
      uint64_t random2 = random.Rand() >> 56;
      compared_l = std::min(random1, random2);
      compared_r = std::max(random1, random2);
      Predicate predicate_1(0, compared_l, compared_r, CompareType::BETWEEN);
      predicates[0] = predicate_1;
      count_if = std::count_if(
          table.begin(), table.end(), [compared_l, compared_r](uint64_t entry) {
            return compared_l <= entry && entry <= compared_r;
          });
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
    }
  }
}

template <size_t COLUMN_COUNT>
void scan_helper_str_singleCol_bin(size_t size,
                                   const std::vector<std::vector<std::string>> &table_portion,
                                   const std::vector<std::set<std::string>> &str_set_vec,
                                   std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> db) {
    Random random;
    for (const auto &table : table_portion) {
        for (size_t i = 0; i < size; ++i) {
            std::vector<Predicate> predicates;
            predicates.reserve(1);
            std::string compared_str_l = std::min(table[i], table[size - i - 1]);
            std::string compared_str_r = std::max(table[i], table[size - i - 1]);

            /// Binary Search
//            auto found_l = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), compared_str_l);
//            uint64_t compared_l = std::distance(str_set_vec[0].begin(), found_l);
//            auto found_r = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), compared_str_r);
//            uint64_t compared_r = std::distance(str_set_vec[0].begin(), found_r);

//            auto compared_l = db->decode_string(compared_str_l, 0);
//            auto compared_r = db->decode_string(compared_str_r, 0);
            predicates.emplace_back(0, compared_str_l, compared_str_r, CompareType::BETWEEN);

            uint64_t count_if = std::count_if(
                    table.begin(), table.end(), [compared_str_l, compared_str_r](std::string entry) {
                        return compared_str_l <= entry && entry <= compared_str_r;
                    });
            EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
        }
    }
}
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
