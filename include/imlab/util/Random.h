// ---------------------------------------------------------------------------
// HyMem
// (c) Alexander van Renen
// ---------------------------------------------------------------------------// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_RANDOM_H_
#define INCLUDE_IMLAB_UTIL_RANDOM_H_
// ---------------------------------------------------------------------------------------------------
#include <cstdint>
#include <limits>
#include "imlab/util/GenericRandom.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace util {
// ---------------------------------------------------------------------------------------------------
// Base on HyMem
// A random number generator, based on Prof. Thomas Neuman's algorithm
class Random : public GenericRandom {
 public:
  explicit Random(uint64_t seed = 88172645463325252ull);
  uint64_t Rand() final;
  float RandScaleFactor();  // returns float between 0 and 1
  uint64_t seed;

  typedef uint64_t result_type;
  static constexpr uint64_t Min() { return 0; }
  static constexpr uint64_t Max() { return std::numeric_limits<uint64_t>::max(); }
  uint64_t operator()() { return Rand(); }
};
// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_RANDOM_H_
// ---------------------------------------------------------------------------------------------------
