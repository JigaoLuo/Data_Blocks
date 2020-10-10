// #define DEBUG_COUNT
// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
/**
 @file
 This file contains some SIMD print function, which are helpful when debugging.
*/
#ifndef INCLUDE_IMLAB_SIMD_PRINT_H_
#define INCLUDE_IMLAB_SIMD_PRINT_H_
// ---------------------------------------------------------------------------------------------------
#include <immintrin.h>
#include <stdio.h>
#include <stdint.h>
#include <bitset>
#include <iostream>
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace SIMD {
/*!
 * print and reinterpret_cast __m256i as some uint8_t
 * @param var
 */
void print256_u8bit(__m256i var);

/*!
 * print and reinterpret_cast __m256i as some uint16_t
 * @param var
 */
void print256_u16bit(__m256i var);

/*!
 * print and reinterpret_cast __m256i as some int32_t
 * @param var
 */
void print256_32bit(__m256i var);

/*!
 * print and reinterpret_cast __m256i as some uint32_t
 * @param var
 */
void print256_u32bit(__m256i var);

/*!
 * print and reinterpret_cast __m256i as some uint32_t as BitSet
 * @param var
 */
void print256_bitset_u32bit(__m256i var);

/*!
 * print and reinterpret_cast __m256i as some uint64_t
 * @param var
 */
void print256_u64bit(__m256i var);

/*!
 * print and reinterpret_cast __m256i as some uint64_t as BitSet
 * @param var
 */
void print256_bitset_u64bit(__m256i var);
// ---------------------------------------------------------------------------------------------------
}  // namespace SIMD
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_SIMD_PRINT_H_
// ---------------------------------------------------------------------------------------------------

