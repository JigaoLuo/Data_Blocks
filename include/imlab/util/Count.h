// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_COUNT_H_
#define INCLUDE_IMLAB_UTIL_COUNT_H_
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
#include "imlab/util/Predicate.h"
#include "imlab/util/MatchTable.h"
#include "imlab/SIMD/Movemask.h"
// #include "imlab/SIMD/Print.hh"
// ---------------------------------------------------------------------------------------------------
// #define DEBUG_COUNT
//#define SCALAR
 #define SIMD_
// ---------------------------------------------------------------------------------------------------
namespace imlab {
// ---------------------------------------------------------------------------------------------------
using SIMD::Greater8u;
using SIMD::LesserOrEqual8u;
using SIMD::GreaterOrEqual8u;
using SIMD::Lesser8u;
using SIMD::_mm256_movemask_epi16;
using SIMD::Greater16u;
using SIMD::Lesser16u;
using SIMD::LesserOrEqual16u;
using SIMD::GreaterOrEqual16u;
// ---------------------------------------------------------------------------------------------------
namespace util {
// ---------------------------------------------------------------------------------------------------
/// @param SIMD_ITERATIONS how many iterations are need in this function
/// @param bitMask bit mask generated from SIMD compare => indicating how many matched and which relative index is match
/// @param scanPos the start offset, with the relative index => build a absolute index
/// @param writer the index vector
template <size_t SIMD_ITERATIONS>
inline void simd_iterations(uint32_t bitMask,
                            uint32_t scanPos,
                            uint32_t* &writer) {
  for (size_t i = 0; i < SIMD_ITERATIONS; ++i) {
    auto& matchEntry = FirstScan_MatchTable[(bitMask >> 8 * i) & 0xFF];
    __m256i scanPosVec = _mm256_set1_epi32(scanPos + 8 * i);  /// Start Offset / Base Offset
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(writer),
                        _mm256_add_epi32(scanPosVec, _mm256_srai_epi32(matchEntry.reg256, 8)));
    writer += static_cast<uint8_t>(matchEntry.cell[0] /* how many macthed => so this pointer be shifted by this # matched*/);
  }
}
// ---------------------------------------------------------------------------------------------------
/// Scan Helper Function for first scan
/// templated with EncodingType E, CompareType C
/// @return # matches in this count function call
/// @param p predicate on specific COLUMN
/// @param dataLength how many byte will be scaned in this function
/// @param column_base_pointer the base pointer, where this COLUMN starts (Not Starting with SMA => Starting direct with DATA)
/// @param index_vector index vector for return information
/// @param sma_ptr SMA containing the min and max
/// @param index_offset the offset added to this SIMD iteration as a BasePoint Index in Datablock
///                     DRAM Datablock with always 0 (SIMD add to EVERY scan value from scan)
/// @param col_count_start_index the first index of col_count to be written
///                     DRAM Datablock with always 0
template <EncodingType E, CompareType C>
static uint32_t first_count(const Predicate &p,
                            const uint32_t dataLength,
                            uint8_t *column_base_pointer,
                            std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                            const SMA *sma_ptr,
                            const uint32_t index_offset,
                            const uint32_t col_count_start_index);
// ---------------------------------------------------------------------------------------------------
/// Scan Helper Function for non first scan
/// templated with EncodingType E, CompareType C
/// @return # matches in this count function call (DIFFEREENT AS THE FIRST SCAN)
/// @param p predicate on specific COLUMN
/// @param num_index how many indexes should be checked
/// @param column_base_pointer the base pointer, where this COLUMN starts (Not Starting with SMA => Starting direct with DATA)
/// @param index_vector index vector for return information
/// @param sma_ptr SMA containing the min and max
/// @param index_offset the offset added to this SIMD iteration as a BasePoint Index in Datablock
///                     DRAM Datablock with always 0 (SIMD add to EVERY scan value from scan)
/// @param col_count_start_index the first index of col_count to be written
///                     DRAM Datablock with always 0
template <EncodingType E, CompareType C>
static uint32_t non_first_count(const Predicate &p,
                                const uint32_t num_index,
                                uint8_t *column_base_pointer,
                                std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                const SMA *sma_ptr,
                                const uint32_t index_offset,
                                const uint32_t col_count_start_index /* writer offset*/,
                                const uint32_t reader_offset);
// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_COUNT_H_
// ---------------------------------------------------------------------------------------------------
