//// ---------------------------------------------------------------------------------------------------
//// IMLAB
//// ---------------------------------------------------------------------------------------------------
//#ifndef INCLUDE_IMLAB_UTIL_COUNT_SINGLEVALUE_H_
//#define INCLUDE_IMLAB_UTIL_COUNT_SINGLEVALUE_H_
//// ---------------------------------------------------------------------------------------------------
//#include <stdint.h>
//#include <assert.h>
//#include <immintrin.h>
//#include <smmintrin.h>
//#include <cstdint>
//#include <functional>
//#include <sstream>
//#include <iostream>
//#include <bitset>
//#include <vector>
//#include <numeric> // std::iota
//#include "imlab/util/Count.h"
//// ---------------------------------------------------------------------------------------------------
//namespace imlab {
//namespace util {
//// ---------------------------------------------------------------------------------------------------
//// Encoding: SingleValue------------------------------------------------------------------------------
//template <>
//void first_count<EncodingType::Singlevalue, CompareType::EQUAL>(const Predicate &p,
//                                                          const uint32_t dataLength,
//                                                          uint8_t *column_base_pointer,
//                                                          std::vector<uint32_t> &col_count,
//                                                          const SMA* sma_ptr,
//                                                          bool first_scan,
//                                                          const uint32_t index_offset) {
//  const auto[single_value, appear_time] = sma_ptr->getSMA_min_max();
//
//  #ifdef DEBUG_COUNT
//    std::cout << "first_count() Singlevalue EQUAL for val: " << p.val << std::endl;
//    printf("first_count() ptr hat value %ld, ", *sma_ptr);
//    printf("hat times %ld, ", *(sma_ptr + 1));
//    printf("hast in pointer %p\n", sma_ptr);
//  #endif
//  if (p.val == single_value) {  // all equal: valid
//    if (col_count.size() == 0) {  // if initial then all in, if initialed change nothing
//      col_count.resize(appear_time);
//      std::generate(col_count.begin(), col_count.end(), [n = 0] () mutable { return n++; });
//    }
//  } else {  // all not equal: invalid
//    if (col_count.size() != 0) {
//      for (uint32_t i = 0; i < col_count.size(); ++i) col_count[i] = UINT32_MAX;
//    }
//  }
//}
//
//template <>
//void first_count<EncodingType::Singlevalue, CompareType::GREATER_THAN>(const Predicate &p,
//                                                                 const uint32_t dataLength,
//                                                                 uint8_t *column_base_pointer,
//                                                                 std::vector<uint32_t> &col_count,
//                                                                 const SMA* sma_ptr,
//                                                                 bool first_scan,
//                                                                 const uint32_t index_offset) {
//  const auto[single_value, appear_time] = sma_ptr->getSMA_min_max();
//
//  #ifdef DEBUG_COUNT
//    std::cout << "first_count() Singlevalue GREATER_THAN for val: " << p.val << std::endl;
//    printf("first_count() ptr hat value %ld, ", *sma_ptr);
//    printf("hat times %ld, ", *(sma_ptr + 1));
//    printf("hat in pointer %p\n", sma_ptr);
//  #endif
//  if (p.val < single_value) {  // all greater than: valid
//    if (col_count.size() == 0) {  // if initial then all in, if initialed change nothing
//      col_count.resize(appear_time);
//      std::generate(col_count.begin(), col_count.end(), [n = 0] () mutable { return n++; });
//    }
//  } else {  // all not greater than: invalid
//    if (col_count.size() != 0) {
//      for (uint32_t i = 0; i < col_count.size(); ++i) col_count[i] = UINT32_MAX;
//    }
//  }
//}
//
//template <>
//void first_count<EncodingType::Singlevalue, CompareType::LESS_THAN>(const Predicate &p,
//                                                              const uint32_t dataLength,
//                                                              uint8_t *column_base_pointer,
//                                                              std::vector<uint32_t> &col_count,
//                                                              const SMA* sma_ptr,
//                                                              bool first_scan,
//                                                              const uint32_t index_offset) {
//  const auto[single_value, appear_time] = sma_ptr->getSMA_min_max();
//
//  #ifdef DEBUG_COUNT
//    std::cout << "first_count() Singlevalue LESS_THAN for val: " << p.val << std::endl;
//    printf("first_count() ptr hat value %ld, ", *sma_ptr);
//    printf("hat times %ld, ", *(sma_ptr + 1));
//    printf("hat in pointer %p\n", sma_ptr);
//  #endif
//  if (p.val > single_value) {  // all less than: valid
//    if (col_count.size() == 0) {  // if initial then all in, if initialed change nothing
//      col_count.resize(appear_time);
//      std::generate(col_count.begin(), col_count.end(), [n = 0] () mutable { return n++; });
//    }
//  } else {  // all not less than: invalid
//    if (col_count.size() != 0) {
//      for (uint32_t i = 0; i < col_count.size(); ++i) col_count[i] = UINT32_MAX;
//    }
//  }
//}
//
//template <>
//void first_count<EncodingType::Singlevalue, CompareType::BETWEEN>(const Predicate &p,
//                                                            const uint32_t dataLength,
//                                                            uint8_t *column_base_pointer,
//                                                            std::vector<uint32_t> &col_count,
//                                                            const SMA* sma_ptr,
//                                                            bool first_scan,
//                                                            const uint32_t index_offset) {
//  const auto[single_value, appear_time] = sma_ptr->getSMA_min_max();
//
//  #ifdef DEBUG_COUNT
//    std::cout << "first_count() Singlevalue BETWEEN for val: " << p.val << "  " << p.right_value << std::endl;
//    printf("first_count() ptr hat value %ld, ", *sma_ptr);
//    printf("hat times %ld, ", *(sma_ptr + 1));
//    printf("hat in pointer %p\n", sma_ptr);
//  #endif
//  if (p.val < single_value && single_value < p.right_value) {  // all between: valid
//    if (col_count.size() == 0) {  // if initial then all in, if initialed change nothing
//      col_count.resize(appear_time);
//      std::generate(col_count.begin(), col_count.end(), [n = 0] () mutable { return n++; });
//    }
//  } else {  // all invalid
//    if (col_count.size() != 0) {
//      for (uint32_t i = 0; i < col_count.size(); ++i) col_count[i] = UINT32_MAX;
//    }
//  }
//}
//// ---------------------------------------------------------------------------------------------------
//}  // namespace util
//// ---------------------------------------------------------------------------------------------------
//}  // namespace imlab
//// ---------------------------------------------------------------------------------------------------
//#endif  // INCLUDE_IMLAB_UTIL_COUNT_SINGLEVALUE_H_
//// ---------------------------------------------------------------------------------------------------
