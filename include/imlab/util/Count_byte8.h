// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_COUNT_BYTE8_H_
#define INCLUDE_IMLAB_UTIL_COUNT_BYTE8_H_
// ---------------------------------------------------------------------------------------------------
#include <stdint.h>
#include <assert.h>
#include <immintrin.h>
#include <smmintrin.h>
#include <cstdint>
#include <functional>
#include <sstream>
#include <iostream>
#include <bitset>
#include <vector>
#include <numeric> // std::iota
#include "imlab/util/Count.h"
#include "imlab/SIMD/SIMD_Types.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
// ---------------------------------------------------------------------------------------------------
using SIMD::_mm256_movemask_epi64;
using SIMD::Greater64u;
using SIMD::Lesser64u;
using SIMD::LesserOrEqual64u;
using SIMD::GreaterOrEqual64u;
using SIMD::SIMD_WIDTH_BYTE8;
using SIMD::SIMD_WIDTH_BYTE8_LOG2;
using SIMD::SIMD_ITERATION_BYTE8;
using SIMD::SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE;
using SIMD::SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1;
// ---------------------------------------------------------------------------------------------------
namespace util {
//TODO: refactoring: each time 4 Uint64_t like frist_count (NOT Performance relevant)
//TODO: maybe process 4 uint64 pro time

// ---------------------------------------------------------------------------------------------------
// Encoding: Byte8------------------------------------------------------------------------------
template <>
uint32_t first_count<EncodingType::Byte8, CompareType::EQUAL>(const Predicate &p,
                                                              const uint32_t dataLength,
                                                              uint8_t *column_base_pointer,
                                                              std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                              const SMA *sma_ptr,
                                                              const uint32_t index_offset,
                                                              const uint32_t col_count_start_index) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  uint64_t* ptr = reinterpret_cast<uint64_t*>(column_base_pointer);
  const uint64_t value = p.val;
  const uint64_t differ = value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(ptr) % 32 == 0) {
    numSimdIterations = (dataLength >> SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1);  /// 4 for sizeof(uint32_t)
  } else {
    while (pre_scalar_counter < dataLength && (uintptr_t)reinterpret_cast<void*>(ptr + pre_scalar_counter) % 32 != 0) {
      const uint32_t value = *(ptr + pre_scalar_counter);
      if (value == differ) {
        *writer = (pre_scalar_counter + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (dataLength - (pre_scalar_counter << 3)) >> (SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1);  /// 4 for sizeof(uint32_t)
  }

  /// Case for first scan => full loop all data needed
  #ifdef SIMD_
  const __m256i comparisonValueVec = _mm256_set1_epi64x(differ);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  __m256i* column_base_pointer_m256 = reinterpret_cast<__m256i*>(ptr + pre_scalar_counter);

  for (uint32_t i = 0; i != numSimdIterations; i++) {
    /// Load and compare 32 values
    __m256i attributeVec = _mm256_load_si256(column_base_pointer_m256 + (i << 1));  /// 2 for sizeof(uint16_t)
    __m256i selMask = _mm256_cmpeq_epi64(attributeVec, comparisonValueVec);  /// compare for equal
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);  /// only the last 4 bit is working, so & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    /// Load and compare 32 values
    attributeVec = _mm256_load_si256(column_base_pointer_m256 + (i << 1) + 1);  /// 2 for sizeof(uint16_t)
    selMask = _mm256_cmpeq_epi64(attributeVec, comparisonValueVec);  /// compare for equal
    uint8_t bitMask_2 = (_mm256_movemask_epi64(selMask) << 4);

    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    const uint32_t scanPos = (i << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + (1 << SIMD_WIDTH_BYTE8_LOG2);  /// uint32_t scanPos = from + (i * simdWidth);
    simd_iterations<SIMD_ITERATION_BYTE8>(bitMask, scanPos + index_offset, writer);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < (dataLength >> UB8_SIZE_LOG2); scalar_i++) {
    const uint64_t value = *(ptr + scalar_i);
    if (value == differ) {
      *writer = scalar_i;
      writer++;
    }
  }

  return (writer - &index_vector[col_count_start_index]);
#endif

  #ifdef SCALAR
  size_t match_counter = 0;
  /// in < dataLength >> 3 == in < dataLength / 8
  /// Reason: Each tuple has 8B uint64_t
  for (size_t in = 0 ; in < dataLength >> 3; in++) {
    if (*(ptr + in) == differ) {
      index_vector[match_counter] = in;
      match_counter++;
    }
  }
  #endif

  #ifdef DEBUG_COUNT
    for (size_t in = 0 ; in < dataLength / 8; in++) {
      if (in == 0) { std::cout << "count() Byte8 EQUAL for val: " << p.val << std::endl; }
      // printf("ptr has value %lu, ", *(ptr + in) + min);
      // printf("ptr has in pointer %p\n", ptr + in);
    }
  return match_counter;
  #endif
}

template <>
uint32_t non_first_count<EncodingType::Byte8, CompareType::EQUAL>(const Predicate &p,
                                                                  const uint32_t num_index,
                                                                  uint8_t *column_base_pointer,
                                                                  std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                  const SMA *sma_ptr,
                                                                  const uint32_t index_offset,
                                                                  const uint32_t col_count_start_index,
                                                                  const uint32_t reader_offset) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  const long long int* ptr = reinterpret_cast<long long int*>(column_base_pointer);  /// should be uint64*, but the _mm256_i32gather_epi64 only allows __int64 const*
  const uint64_t value = p.val;
  const uint64_t differ = value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset]) % 32 == 0) {
    numSimdIterations = num_index >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  } else {
    while (pre_scalar_counter < num_index
           && (uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset + pre_scalar_counter]) % 32 != 0) {
      const uint32_t index = index_vector[reader_offset + pre_scalar_counter] - index_offset;
      const uint64_t value = *(ptr + index);
      if (value == differ) {
        *writer = (index + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (num_index - pre_scalar_counter) >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  }

  const __m128i* index_vector_ptr_mm128 = reinterpret_cast<__m128i*>(&index_vector[reader_offset + pre_scalar_counter]);

  const __m256i* index_vector_ptr_mm256 = reinterpret_cast<__m256i*>(&index_vector[reader_offset + pre_scalar_counter]);


  /// Case for non first scan
#ifdef SIMD_
  const __m256i comparisonValueVec = _mm256_set1_epi64x(differ);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  /// Offset of index, in order to get the data in this page
  const __m128i index_offset_128 = _mm_set1_epi32(index_offset);

  /// Each time 8 uint64_t = 256 bit
  /// Each i = 256 bit
  for (uint32_t i = 0; i != numSimdIterations; i++) {
    __m256i indexes_256 = *(index_vector_ptr_mm256 + i /* reader */);  /// x/4d &indexes

    /// Load 4 Indexes from Index Vector, subtract the offset to get real index in the page
    __m128i indexes = *(index_vector_ptr_mm128 + (i << 1) /* reader */);  /// x/4d &indexes
    __m128i real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    __m256i gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    __m256i selMask = _mm256_cmpeq_epi64(gather, comparisonValueVec);  /// compare for _mm256_cmpeq_epi64
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);  /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    indexes = *(index_vector_ptr_mm128 + (i << 1) + 1/* reader */);  /// x/4d &indexes
    real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    selMask = _mm256_cmpeq_epi64(gather, comparisonValueVec);  /// compare for _mm256_cmpeq_epi64
    uint8_t bitMask_2 = (_mm256_movemask_epi64(selMask) << 4);

    /// Merge to a 8bit bitMask
    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    auto& matchEntry = NON_FirstScan_MatchTable[bitMask];
    auto dest = _mm256_permutevar8x32_epi32(indexes_256, matchEntry.reg256);  /// x/8d &dest
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(writer), dest);
    writer += __builtin_popcount(bitMask);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + reader_offset + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < num_index + reader_offset; scalar_i++) {
    const uint32_t index = index_vector[scalar_i] - index_offset;
    const uint64_t value = *(ptr + index);
    if (value == differ) {
      *writer = (index + index_offset);
      writer++;
    }
  }
  return (writer - &index_vector[col_count_start_index]);
#endif
}

template <>
uint32_t first_count<EncodingType::Byte8, CompareType::GREATER_THAN>(const Predicate &p,
                                                                     const uint32_t dataLength,
                                                                     uint8_t *column_base_pointer,
                                                                     std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                     const SMA *sma_ptr,
                                                                     const uint32_t index_offset,
                                                                     const uint32_t col_count_start_index) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  uint64_t* ptr = reinterpret_cast<uint64_t*>(column_base_pointer);
  const uint64_t value = p.val;
  const uint64_t differ = value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(ptr) % 32 == 0) {
    numSimdIterations = (dataLength >> SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1);  /// 4 for sizeof(uint32_t)
  } else {
    while (pre_scalar_counter < dataLength && (uintptr_t)reinterpret_cast<void*>(ptr + pre_scalar_counter) % 32 != 0) {
      const uint32_t value = *(ptr + pre_scalar_counter);
      if (value > differ) {
        *writer = (pre_scalar_counter + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (dataLength - (pre_scalar_counter << 3)) >> (SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1);  /// 4 for sizeof(uint32_t)
  }

  /// Case for first scan => full loop all data needed
  #ifdef SIMD_
  const __m256i comparisonValueVec = _mm256_set1_epi64x(differ);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  __m256i* column_base_pointer_m256 = reinterpret_cast<__m256i*>(ptr + pre_scalar_counter);

  for (uint32_t i = 0; i != numSimdIterations; i++) {
    /// Load and compare 32 values
    __m256i attributeVec = _mm256_load_si256(column_base_pointer_m256 + (i << 1));  /// 8 for sizeof(uint64_t)
    __m256i selMask = Greater64u(attributeVec, comparisonValueVec);  /// compare for equal
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);  /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    /// Load and compare 32 values
    attributeVec = _mm256_load_si256(column_base_pointer_m256 + (i << 1) + 1);  /// 8 for sizeof(uint64_t)
     selMask = Greater64u(attributeVec, comparisonValueVec);  /// compare for equal
    uint8_t bitMask_2 = (_mm256_movemask_epi64(selMask) << 4);

    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    const uint32_t scanPos = (i << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + (1 << SIMD_WIDTH_BYTE8_LOG2);  /// uint32_t scanPos = from + (i * simdWidth);
    simd_iterations<SIMD_ITERATION_BYTE8>(bitMask, scanPos - 4 + index_offset, writer);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < (dataLength >> UB8_SIZE_LOG2); scalar_i++) {
    const uint64_t value = *(ptr + scalar_i);
    if (value > differ) {
      *writer = scalar_i;
      writer++;
    }
  }

  return (writer - &index_vector[col_count_start_index]);
#endif

  #ifdef SCALAR
  size_t match_counter = 0;
  /// in < dataLength >> 3 == in < dataLength / 8
  /// Reason: Each tuple has 8B uint64_t
  for (size_t in = 0 ; in < (dataLength >> UB8_SIZE_LOG2); in++) {
    if (*(ptr + in) > differ) {
      index_vector[match_counter] = in;
      match_counter++;
    }
  }
  return match_counter;
#endif
  #ifdef DEBUG_COUNT
    for (size_t in = 0 ; in < dataLength / 8; in++) {
      if (in == 0) { std::cout << "count() Byte8 GREATER_THAN for val: " << p.val << std::endl;}
      // printf("ptr has value %lu, ", *(ptr + in) + min);
      // printf("ptr has in pointer %p\n", ptr + in);
    }
  #endif
}


template <>
uint32_t non_first_count<EncodingType::Byte8, CompareType::GREATER_THAN>(const Predicate &p,
                                                                         const uint32_t num_index,
                                                                         uint8_t *column_base_pointer,
                                                                         std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                         const SMA *sma_ptr,
                                                                         const uint32_t index_offset,
                                                                         const uint32_t col_count_start_index,
                                                                         const uint32_t reader_offset) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  const long long int* ptr = reinterpret_cast<long long int*>(column_base_pointer);  /// should be uint64*, but the _mm256_i32gather_epi64 only allows __int64 const*
  const uint64_t value = p.val;
  const uint64_t differ = value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset]) % 32 == 0) {
    numSimdIterations = num_index >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  } else {
    while (pre_scalar_counter < num_index
           && (uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset + pre_scalar_counter]) % 32 != 0) {
      const uint32_t index = index_vector[reader_offset + pre_scalar_counter] - index_offset;
      const uint64_t value = *(ptr + index);
      if (value > differ) {
        *writer = (index + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (num_index - pre_scalar_counter) >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  }

  const __m128i* index_vector_ptr_mm128 = reinterpret_cast<__m128i*>(&index_vector[reader_offset + pre_scalar_counter]);

  const __m256i* index_vector_ptr_mm256 = reinterpret_cast<__m256i*>(&index_vector[reader_offset + pre_scalar_counter]);

/// Case for non first scan
#ifdef SIMD_
  const __m256i comparisonValueVec = _mm256_set1_epi64x(differ);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  /// Offset of index, in order to get the data in this page
  const __m128i index_offset_128 = _mm_set1_epi32(index_offset);

  /// Each time 8 uint64_t = 256 bit
  /// Each i = 256 bit
  for (uint32_t i = 0; i != numSimdIterations; i++) {
    __m256i indexes_256 = *(index_vector_ptr_mm256 + i /* reader */);  /// x/4d &indexes

    /// Load 4 Indexes from Index Vector, subtract the offset to get real index in the page
    __m128i indexes = *(index_vector_ptr_mm128 + (i << 1) /* reader */);  /// x/4d &indexes
    __m128i real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    __m256i gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    __m256i selMask = Greater64u(gather, comparisonValueVec);  /// compare for Greater32u
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);  /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    indexes = *(index_vector_ptr_mm128 + (i << 1) + 1/* reader */);  /// x/4d &indexes
    real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    selMask = Greater64u(gather, comparisonValueVec);  /// compare for Greater32u
    uint8_t bitMask_2 = (_mm256_movemask_epi64(selMask) << 4);

    /// Merge to a 8bit bitMask
    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    auto& matchEntry = NON_FirstScan_MatchTable[bitMask];
    auto dest = _mm256_permutevar8x32_epi32(indexes_256, matchEntry.reg256);  /// x/8d &dest
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(writer), dest);
    writer += __builtin_popcount(bitMask);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + reader_offset + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < num_index + reader_offset; scalar_i++) {
      const uint32_t index = index_vector[scalar_i] - index_offset;
      const uint64_t value = *(ptr + index);
      if (value > differ) {
        *writer = (index + index_offset);
        writer++;
      }
  }
  return (writer - &index_vector[col_count_start_index]);
#endif
}

template <>
uint32_t first_count<EncodingType::Byte8, CompareType::LESS_THAN>(const Predicate &p,
                                                                  const uint32_t dataLength,
                                                                  uint8_t *column_base_pointer,
                                                                  std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                  const SMA *sma_ptr,
                                                                  const uint32_t index_offset,
                                                                  const uint32_t col_count_start_index) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  const uint64_t value = p.val;
  uint64_t* ptr = reinterpret_cast<uint64_t*>(column_base_pointer);
  const uint64_t differ = value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(ptr) % 32 == 0) {
    numSimdIterations = (dataLength >> SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1);  /// 4 for sizeof(uint32_t)
  } else {
    while (pre_scalar_counter < dataLength && (uintptr_t)reinterpret_cast<void*>(ptr + pre_scalar_counter) % 32 != 0) {
      const uint32_t value = *(ptr + pre_scalar_counter);
      if (value < differ) {
        *writer = (pre_scalar_counter + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (dataLength - (pre_scalar_counter << 3)) >> (SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE_PLUS1);  /// 4 for sizeof(uint32_t)
  }

  /// Case for first scan => full loop all data needed
    #ifdef SIMD_

  const __m256i comparisonValueVec = _mm256_set1_epi64x((uint64_t)differ);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  __m256i* column_base_pointer_m256 = reinterpret_cast<__m256i*>(ptr + pre_scalar_counter);

  for (uint32_t i = 0; i != numSimdIterations; i++) {
    /// Load and compare 32 values
    __m256i attributeVec = _mm256_load_si256(column_base_pointer_m256 + (i << 1));  /// 8 for sizeof(uint64_t)
    __m256i selMask = Lesser64u(attributeVec, comparisonValueVec);  /// compare for Lesser64u
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);  /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    /// Load and compare 32 values
    attributeVec = _mm256_load_si256(column_base_pointer_m256 + (i << 1) + 1);  /// 8 for sizeof(uint64_t)
    selMask = Lesser64u(attributeVec, comparisonValueVec);  /// compare for Lesser64u
    uint8_t bitMask_2 = (_mm256_movemask_epi64(selMask) << 4);

    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    const uint32_t scanPos = (i << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + (1 << SIMD_WIDTH_BYTE8_LOG2);  /// uint32_t scanPos = from + (i * simdWidth);
    simd_iterations<SIMD_ITERATION_BYTE8>(bitMask, scanPos - 4 + index_offset, writer);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < (dataLength >> UB8_SIZE_LOG2); scalar_i++) {
    const uint64_t value = *(ptr + scalar_i);
    if (value < differ) {
      *writer = scalar_i;
      writer++;
    }
  }
  return (writer - &index_vector[col_count_start_index]);
#endif

  #ifdef SCALAR
  size_t match_counter = 0;
  /// in < dataLength >> 3 == in < dataLength / 8
  /// Reason: Each tuple has 8B uint64_t
  for (size_t in = 0 ; in < (dataLength >> UB8_SIZE_LOG2); in++) {
    if (*(ptr + in) < differ) {
      index_vector[match_counter] = in;
      match_counter++;
    }
  }
    return match_counter;
#endif

  #ifdef DEBUG_COUNT
    for (size_t in = 0 ; in < dataLength / 8; in++) {
      if (in == 0) { std::cout << "count() Byte8 LESS_THAN for val: " << p.val << std::endl; }
      // printf("ptr has value %lu, ", *(ptr + in) + min);
      // printf("ptr has in pointer %p\n", ptr + in);
    }
  #endif
}

template <>
uint32_t non_first_count<EncodingType::Byte8, CompareType::LESS_THAN>(const Predicate &p,
                                                                      const uint32_t num_index,
                                                                      uint8_t *column_base_pointer,
                                                                      std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                      const SMA *sma_ptr,
                                                                      const uint32_t index_offset,
                                                                      const uint32_t col_count_start_index,
                                                                      const uint32_t reader_offset) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  const long long int* ptr = reinterpret_cast<long long int*>(column_base_pointer);  /// should be uint64*, but the _mm256_i32gather_epi64 only allows __int64 const*
  const uint64_t value = p.val;
  const uint64_t differ = value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset]) % 32 == 0) {
    numSimdIterations = num_index >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  } else {
    while (pre_scalar_counter < num_index
           && (uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset + pre_scalar_counter]) % 32 != 0) {
      const uint32_t index = index_vector[reader_offset + pre_scalar_counter] - index_offset;
      const uint64_t value = *(ptr + index);
      if (value < differ) {
        *writer = (index + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (num_index - pre_scalar_counter) >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  }

  const __m128i* index_vector_ptr_mm128 = reinterpret_cast<__m128i*>(&index_vector[reader_offset + pre_scalar_counter]);

  const __m256i* index_vector_ptr_mm256 = reinterpret_cast<__m256i*>(&index_vector[reader_offset + pre_scalar_counter]);

  /// Case for non first scan
#ifdef SIMD_
  const __m256i comparisonValueVec = _mm256_set1_epi64x(differ);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  /// Offset of index, in order to get the data in this page
  const __m128i index_offset_128 = _mm_set1_epi32(index_offset);

  /// Each time 8 uint64_t = 256 bit
  /// Each i = 256 bit
  for (uint32_t i = 0; i != numSimdIterations; i++) {
    __m256i indexes_256 = *(index_vector_ptr_mm256 + i /* reader */);  /// x/4d &indexes

    /// Load 4 Indexes from Index Vector, subtract the offset to get real index in the page
    __m128i indexes = *(index_vector_ptr_mm128 + (i << 1) /* reader */);  /// x/4d &indexes
    __m128i real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    __m256i gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    __m256i selMask = Lesser64u(gather, comparisonValueVec);  /// compare for Lesser64u
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);    /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    indexes = *(index_vector_ptr_mm128 + (i << 1) + 1/* reader */);  /// x/4d &indexes
    real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    selMask = Lesser64u(gather, comparisonValueVec);  /// compare for Lesser64u
    uint8_t bitMask_2 = (_mm256_movemask_epi64(selMask) << 4);

    /// Merge to a 8bit bitMask
    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    auto& matchEntry = NON_FirstScan_MatchTable[bitMask];
    auto dest = _mm256_permutevar8x32_epi32(indexes_256, matchEntry.reg256);  /// x/8d &dest
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(writer), dest);
    writer += __builtin_popcount(bitMask);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + reader_offset + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < num_index + reader_offset; scalar_i++) {
    const uint32_t index = index_vector[scalar_i] - index_offset;
    const uint64_t value = *(ptr + index);
    if (value < differ) {
      *writer = (index + index_offset);
      writer++;
    }
  }
  return (writer - &index_vector[col_count_start_index]);
#endif
}

template <>
uint32_t first_count<EncodingType::Byte8, CompareType::BETWEEN>(const Predicate &p,
                                                                const uint32_t dataLength,
                                                                uint8_t *column_base_pointer,
                                                                std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                const SMA *sma_ptr,
                                                                const uint32_t index_offset,
                                                                const uint32_t col_count_start_index) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  const uint64_t left_value = p.val;
  const uint64_t right_value = p.right_value;
  uint64_t* ptr = reinterpret_cast<uint64_t*>(column_base_pointer);
  const uint64_t differ_left = (left_value < min)? 0 : (left_value - min);
  const uint64_t differ_right = right_value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(ptr) % 32 == 0) {
    numSimdIterations = (dataLength >> SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE);  /// 4 for sizeof(uint32_t)
  } else {
    while (pre_scalar_counter < dataLength && (uintptr_t)reinterpret_cast<void*>(ptr + pre_scalar_counter) % 32 != 0) {
      const uint32_t value = *(ptr + pre_scalar_counter);
      if (differ_left <= value  && value <= differ_right) {
        *writer = (pre_scalar_counter + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (dataLength - (pre_scalar_counter << 3)) >> (SIMD_WIDTH_BYTE8_LOG2_MUL_SIZE);  /// 4 for sizeof(uint32_t)
  }

  /// Case for first scan => full loop all data needed
  #ifdef SIMD_
  /// Process 4 UINT64 pro time
  const __m256i comparisonValueVec_left = _mm256_set1_epi64x(differ_left);
  const __m256i comparisonValueVec_right = _mm256_set1_epi64x(differ_right);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  __m256i* column_base_pointer_m256 = reinterpret_cast<__m256i*>(ptr + pre_scalar_counter);

  for (uint32_t i = 0; i != numSimdIterations; i++) {
    /// Load and compare 32 values
    __m256i attributeVec = _mm256_load_si256(column_base_pointer_m256 + i);  /// __m256i attributeVec=_mm256_load_si256(reinterpret_cast<__m256i*>(&column[scanPos]));
    __m256i selMask_left = GreaterOrEqual64u(attributeVec, comparisonValueVec_left);  /// compare for great than
    uint8_t bitMask_left = _mm256_movemask_epi64(selMask_left);

    __m256i selMask_right = LesserOrEqual64u(attributeVec, comparisonValueVec_right);  /// compare for less than
    uint8_t bitMask = _mm256_movemask_epi64(selMask_right) & bitMask_left;

    /// Lookup match positions and update positions vector
    const uint32_t scanPos = (i << SIMD_WIDTH_BYTE8_LOG2);  /// uint32_t scanPos = from + (i * simdWidth);
    simd_iterations<SIMD_ITERATION_BYTE8>(bitMask, scanPos + index_offset, writer);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << SIMD_WIDTH_BYTE8_LOG2) + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < (dataLength >> UB8_SIZE_LOG2); scalar_i++) {
    const uint64_t value = *(ptr + scalar_i);
    if (differ_left <= value  && value <= differ_right) {
      *writer = scalar_i;
      writer++;
    }
  }

  return (writer - &index_vector[col_count_start_index]);
#endif

  #ifdef SCALAR
  size_t match_counter = 0;
  /// in < dataLength >> 3 == in < dataLength / 8
  /// Reason: Each tuple has 8B uint64_t
    for (size_t in = 0 ; in < (dataLength >> UB8_SIZE_LOG2); in++) {
      uint64_t value = *(ptr + in);
      if (differ_left <= value  && value <= differ_right) {
        index_vector[match_counter] = in;
        match_counter++;
      }
    }
  return match_counter;
#endif
  #ifdef DEBUG_COUNT
    std::cout << "count() Byte8 BETWEEN for val: " << p.val  << "  " << p.right_value << std::endl;
    for (size_t in = 0 ; in < dataLength / 8; in++) {
      printf("ptr has value %lu, ", *(ptr + in) + min);
      printf("ptr has actually value - min %lu, ", *(ptr + in));
      printf("ptr has in pointer %p\n", ptr + in);
    }
  #endif
}

template <>
uint32_t non_first_count<EncodingType::Byte8, CompareType::BETWEEN>(const Predicate &p,
                                                                    const uint32_t num_index,
                                                                    uint8_t *column_base_pointer,
                                                                    std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                    const SMA *sma_ptr,
                                                                    const uint32_t index_offset,
                                                                    const uint32_t col_count_start_index,
                                                                    const uint32_t reader_offset) {
  const auto[min, max] = sma_ptr->getSMA_min_max();
  const long long int* ptr = reinterpret_cast<long long int*>(column_base_pointer);  /// should be uint64*, but the _mm256_i32gather_epi64 only allows __int64 const*

  const uint64_t left_value = p.val;
  const uint64_t right_value = p.right_value;
  const uint64_t differ_left = (left_value < min)? 0 : (left_value - min);
  const uint64_t differ_right = right_value - min;

  uint32_t* writer = &index_vector[col_count_start_index];

  /// IF Not 32 aligned, THEN process then using scalar
  uint32_t numSimdIterations = 0;
  uint8_t pre_scalar_counter = 0;
  if ((uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset]) % 32 == 0) {
    numSimdIterations = num_index >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  } else {
    while (pre_scalar_counter < num_index
           && (uintptr_t)reinterpret_cast<void*>(&index_vector[reader_offset + pre_scalar_counter]) % 32 != 0) {
      const uint32_t index = index_vector[reader_offset + pre_scalar_counter] - index_offset;
      const uint64_t value = *(ptr + index);
      if (differ_left <= value  && value <= differ_right) {
        *writer = (index + index_offset);
        writer++;
      }
      pre_scalar_counter++;
    }
    numSimdIterations = (num_index - pre_scalar_counter) >> (SIMD_WIDTH_BYTE8_LOG2 + 1);  /// Each time 8 uint64_t = 256 bit
  }

  const __m128i* index_vector_ptr_mm128 = reinterpret_cast<__m128i*>(&index_vector[reader_offset + pre_scalar_counter]);

  const __m256i* index_vector_ptr_mm256 = reinterpret_cast<__m256i*>(&index_vector[reader_offset + pre_scalar_counter]);


  /// Case for non first scan
#ifdef SIMD_
  const __m256i comparisonValueVec_left = _mm256_set1_epi64x(differ_left);
  const __m256i comparisonValueVec_right = _mm256_set1_epi64x(differ_right);
  assert((uintptr_t)reinterpret_cast<void*>(column_base_pointer) % 32 == 0);  /// get datablock->data 32 bytes align for AVX

  /// Offset of index, in order to get the data in this page
  const __m128i index_offset_128 = _mm_set1_epi32(index_offset);

  for (uint32_t i = 0; i != numSimdIterations; i++) {
    __m256i indexes_256 = *(index_vector_ptr_mm256 + i /* reader */);  /// x/4d &indexes

    /// Load 4 Indexes from Index Vector, subtract the offset to get real index in the page
    __m128i indexes = *(index_vector_ptr_mm128 + (i << 1) /* reader */);  /// x/4d &indexes
    __m128i real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    __m256i gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    __m256i selMask = GreaterOrEqual64u(gather, comparisonValueVec_left);  /// compare for GreaterOrEqual64u
    uint8_t bitMask_1 = _mm256_movemask_epi64(selMask);  /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    /// compare 4 values
    selMask = LesserOrEqual64u(gather, comparisonValueVec_right);  /// compare for LesserOrEqual64u
    bitMask_1 = (_mm256_movemask_epi64(selMask) & bitMask_1);

    indexes = *(index_vector_ptr_mm128 + (i << 1) + 1/* reader */);  /// x/4d &indexes
    real_indexes = _mm_sub_epi32(indexes, index_offset_128);

    /// Load 4 Int64 at the 4 indexes from Datablock
    gather = _mm256_i32gather_epi64(ptr, real_indexes, 8);     /// x/4u &gather

    /// compare 4 values
    selMask = GreaterOrEqual64u(gather, comparisonValueVec_left);  /// compare for GreaterOrEqual64u
    uint8_t bitMask_2 = _mm256_movemask_epi64(selMask);  /// & 0x0F could be removed from: _mm256_movemask_epi64(selMask) & 0x0F

    /// compare 4 values
    selMask = LesserOrEqual64u(gather, comparisonValueVec_right);  /// compare for Lesser64u
    bitMask_2 = ((_mm256_movemask_epi64(selMask) & bitMask_2)<< 4);

    /// Merge to a 8bit bitMask
    uint8_t bitMask = bitMask_2 + bitMask_1;

    /// Lookup match positions and update positions vector
    auto& matchEntry = NON_FirstScan_MatchTable[bitMask];
    auto dest = _mm256_permutevar8x32_epi32(indexes_256, matchEntry.reg256);  /// x/8d &dest
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(writer), dest);
    writer += __builtin_popcount(bitMask);
  }

  /// Rest in scalar code, scan the rest
  const uint32_t scalars_start_index = (numSimdIterations << (SIMD_WIDTH_BYTE8_LOG2 + 1)) + reader_offset + pre_scalar_counter;
  for (uint32_t scalar_i = scalars_start_index; scalar_i < num_index + reader_offset; scalar_i++) {
    const uint32_t index = index_vector[scalar_i] - index_offset;
    const uint64_t value = *(ptr + index);
    if (differ_left <= value  && value <= differ_right) {
      *writer = (index + index_offset);
      writer++;
    }
  }
  return (writer - &index_vector[col_count_start_index]);
#endif
}
// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_COUNT_BYTE8_H_
// ---------------------------------------------------------------------------------------------------
