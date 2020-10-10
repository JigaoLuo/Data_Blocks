// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_PREDICATE_H_
#define INCLUDE_IMLAB_UTIL_PREDICATE_H_
// --------------------------------------------------------------------------------------------------
#include <assert.h>
#include <iostream>
#include "imlab/util/DataBlock_Types.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace util {
// ---------------------------------------------------------------------------------------------------
/// For:
/// CompareType::EQUAL;
/// CompareType::GREATER_THAN;
/// CompareType::LESS_THAN;
/// CompareType::Between
class Predicate {
 public:
  uint32_t col_id;

  /// For: CompareType::EQUAL, CompareType::GREATER_THAN, CompareType::LESS_THAN;
  uint64_t val;

  /// For: CompareType::Between
  uint64_t right_value;

  /// For string with dictionary
  std::string str;

  std::string right_str;

  CompareType compareType;

  Predicate(uint32_t col_id, const std::string& str, CompareType compareType) : col_id(col_id), str(str), compareType(compareType) {}

  Predicate(uint32_t col_id, const std::string& str, const std::string& right_str, CompareType compareType) : col_id(col_id), str(str), right_str(right_str), compareType(compareType) {
    assert(str < right_str);
  }

  Predicate(uint32_t col_id, uint64_t val, CompareType compareType) : col_id(col_id), val(val), right_value(0), compareType(compareType) {}

  Predicate(uint32_t col_id, uint64_t val, uint64_t right_value, CompareType compareType) : col_id(col_id), val(val), right_value(right_value), compareType(compareType) {
    assert(val < right_value);
  }
};
//// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_PREDICATE_H_
// ---------------------------------------------------------------------------------------------------
