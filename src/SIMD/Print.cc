// ---------------------------------------------------------------------------------------------------
#include "imlab/SIMD/Print.h"
// ---------------------------------------------------------------------------------------------------
void imlab::SIMD::print256_u8bit(__m256i var) {
  const uint8_t *val = reinterpret_cast<uint8_t*>(&var);
  for (size_t i = 0; i < 32; ++i) printf("%d  ", val[i]);
  printf("\n");
}

void imlab::SIMD::print256_u16bit(__m256i var) {
  uint16_t *val = reinterpret_cast<uint16_t*>(&var);
  for (size_t i = 0; i < 16; ++i) printf("%d  ", val[i]);
  printf("\n");
}

void imlab::SIMD::print256_32bit(__m256i var) {
  int32_t *val = reinterpret_cast<int32_t*>(&var);
  for (size_t i = 0; i < 8; ++i) std::cout << " " << val[i] << " ";
  printf("\n");
}

void imlab::SIMD::print256_u32bit(__m256i var) {
  uint32_t *val = reinterpret_cast<uint32_t*>(&var);
  for (size_t i = 0; i < 8; ++i) std::cout << " " << val[i] << " ";
  printf("\n");
}

void imlab::SIMD::print256_bitset_u32bit(__m256i var) {
  uint32_t *val = reinterpret_cast<uint32_t*>(&var);
  for (size_t i = 0; i < 8; ++i) std::cout << std::bitset<32>(val[i]) << "  ";
  printf("\n");
}

void imlab::SIMD::print256_u64bit(__m256i var) {
  uint64_t *val = reinterpret_cast<uint64_t*>(&var);
  for (size_t i = 0; i < 4; ++i) printf("%lu  ", val[i]);
  printf("\n");
}

void imlab::SIMD::print256_bitset_u64bit(__m256i var) {
    uint64_t *val = reinterpret_cast<uint64_t*>(&var);
    for (size_t i = 0; i < 4; ++i) std::cout << std::bitset<64>(val[i]) << "  ";
    printf("\n");
}
// ---------------------------------------------------------------------------------------------------
