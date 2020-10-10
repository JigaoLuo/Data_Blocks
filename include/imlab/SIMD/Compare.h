// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
/**
 @file
 This file contains some SIMD compare functions.
 Get Idea from here https://github.com/ermig1979/Simd
 https://github.com/ermig1979/Simd/blob/6c68eb65b7813a17a23880242b4adde4472d6a71/src/Simd/SimdCompare.h
*/
#ifndef INCLUDE_IMLAB_SIMD_COMPARE_H_
#define INCLUDE_IMLAB_SIMD_COMPARE_H_
// ---------------------------------------------------------------------------------------------------
#include <pmmintrin.h>
#include <immintrin.h>
#include <stdint.h>
// ---------------------------------------------------------------------------------------------------
#define SIMD_INLINE inline __attribute__ ((always_inline))    // __forceinline
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace SIMD {

const __m256i K_INV_ZERO_8bit = _mm256_set1_epi8(-1);

const __m256i K_INV_ZERO_16bit = _mm256_set1_epi16(-1);

const __m256i K_INV_ZERO_32bit = _mm256_set1_epi32(-1);

const __m256i K_INV_ZERO_64bit = _mm256_set1_epi64x(-1);

SIMD_INLINE __m256i Greater8u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi8(_mm256_min_epu8(a, b), a), K_INV_ZERO_8bit);
}

SIMD_INLINE __m256i GreaterOrEqual8u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi8(_mm256_max_epu8(a, b), a);
}

SIMD_INLINE __m256i Lesser8u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi8(_mm256_max_epu8(a, b), a), K_INV_ZERO_8bit);
}

SIMD_INLINE __m256i LesserOrEqual8u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi8(_mm256_min_epu8(a, b), a);
}

SIMD_INLINE __m256i Greater16u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi16(_mm256_min_epu16(a, b), a), K_INV_ZERO_16bit);
}

SIMD_INLINE __m256i GreaterOrEqual16u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi16(_mm256_max_epu16(a, b), a);
}

SIMD_INLINE __m256i Lesser16u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi16(_mm256_max_epu16(a, b), a), K_INV_ZERO_16bit);
}

SIMD_INLINE __m256i LesserOrEqual16u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi16(_mm256_min_epu16(a, b), a);
}

SIMD_INLINE __m256i Greater32u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi32(_mm256_min_epu32(a, b), a), K_INV_ZERO_32bit);
}

SIMD_INLINE __m256i GreaterOrEqual32u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi32(_mm256_max_epu32(a, b), a);
}

SIMD_INLINE __m256i Lesser32u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi32(_mm256_max_epu32(a, b), a), K_INV_ZERO_32bit);
}

SIMD_INLINE __m256i LesserOrEqual32u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi32(_mm256_min_epu32(a, b), a);
}

SIMD_INLINE __m256i _mm256_blendv_epi64(__m256i a, __m256i b, __m256i mask) {
  return _mm256_castpd_si256(_mm256_blendv_pd(
      _mm256_castsi256_pd(a), _mm256_castsi256_pd(b), _mm256_castsi256_pd(mask)));
}

SIMD_INLINE __m256i __my_mm256_min_epu64_(__m256i a, __m256i b) {
  uint64_t *val_a = reinterpret_cast<uint64_t*>(&a);
  uint64_t *val_b = reinterpret_cast<uint64_t*>(&b);
  uint64_t e[4];
  for (size_t i = 0; i < 4; ++i) e[i] = (*(val_a + i) < *(val_b + i)) ? *(val_a + i) : *(val_b + i);
  return _mm256_set_epi64x(e[3], e[2], e[1], e[0]);
}

SIMD_INLINE __m256i __my_mm256_max_epu64_(__m256i a, __m256i b) {
  uint64_t *val_a = reinterpret_cast<uint64_t*>(&a);
  uint64_t *val_b = reinterpret_cast<uint64_t*>(&b);
  uint64_t e[4];
  for (size_t i = 0; i < 4; ++i) e[i] = (*(val_a + i) > *(val_b + i)) ? *(val_a + i) : *(val_b + i);
  return _mm256_set_epi64x(e[3], e[2], e[1], e[0]);
}

SIMD_INLINE __m256i Greater64u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi64(__my_mm256_min_epu64_(a, b), a), K_INV_ZERO_64bit);
}

SIMD_INLINE __m256i GreaterOrEqual64u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi64(__my_mm256_max_epu64_(a, b), a);
}

SIMD_INLINE __m256i Lesser64u(__m256i a, __m256i b) {
  return _mm256_andnot_si256(_mm256_cmpeq_epi64(__my_mm256_max_epu64_(a, b), a), K_INV_ZERO_64bit);
}

SIMD_INLINE __m256i LesserOrEqual64u(__m256i a, __m256i b) {
  return _mm256_cmpeq_epi64(__my_mm256_min_epu64_(a, b), a);
}
// ---------------------------------------------------------------------------------------------------
}  // namespace SIMD
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_SIMD_COMPARE_H_
// ---------------------------------------------------------------------------------------------------


