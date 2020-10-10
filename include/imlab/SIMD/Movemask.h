// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
/**
 @file
 This file contains some SIMD function not available on the CPU, which only support upto AVX256.
 These function are supported and similar implemented as in the AVC512.
*/
#ifndef INCLUDE_IMLAB_SIMD_MOVEMASK_H_
#define INCLUDE_IMLAB_SIMD_MOVEMASK_H_
// ---------------------------------------------------------------------------------------------------
#include <pmmintrin.h>
#include <immintrin.h>
#include <stdint.h>
#include "imlab/SIMD/Compare.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace SIMD {
inline uint16_t oddBits(uint32_t big) {
  uint16_t small = 0;
  for (int p = 0, q = 0; p < 16; p++, q += 2) small |= (big & (1 << q)) >> p;
  return small;
}

inline uint16_t evenBits(uint32_t big) {
  return oddBits(big >> 1);
}

SIMD_INLINE uint16_t _mm256_movemask_epi16(__m256i a) {
  return oddBits(_mm256_movemask_epi8(a));
}

SIMD_INLINE uint8_t _mm256_movemask_epi32(__m256i a) {
  return (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(a));
}

SIMD_INLINE uint8_t _mm256_movemask_epi64(__m256i a) {
  return (uint8_t)_mm256_movemask_pd(_mm256_castsi256_pd(a));
}
// ---------------------------------------------------------------------------------------------------
}  // namespace SIMD
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_SIMD_MOVEMASK_H_
// ---------------------------------------------------------------------------------------------------


