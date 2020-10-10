// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_DATABLOCK_SMA_H_
#define INCLUDE_IMLAB_DATABLOCK_SMA_H_
// ---------------------------------------------------------------------------------------------------
#include <assert.h>
#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <utility>
#include <algorithm>
// ---------------------------------------------------------------------------------------------------
namespace imlab {
/**
 *  "(iv) Data Blocks further contain Small Materialized Aggregates (SMAs)
 *  that include a minimum and a maximum value for each attribute in the Data Block, 
 *  which can be used to determine if a Data Block can be skipped during a scan"
 *
 *  Here I use a uint64_t SMD containing 2 uint64_t, representing a MIN and a MAX
 */
class SMA {
 private:
  /// For singlevalue is a exception
  /// min: for the value (singlevalue)
  /// max: the times this singlevalue appears (same as the tuple count or column size)

  /** 
   * SMA min represents the min for each attribute in this DataBlock
   */
  uint64_t min;

  /** 
   * SMA max represents the max for each attribute in this DataBlock
   */
  uint64_t max;

 public:
  /**
   *  Default Constructor
   */
  SMA() : min(0), max(0) {}

  /**
   *  Constructor
   */
  SMA(uint64_t min, uint64_t max) : min(min), max(max) { assert(min <= max); }

  /**
   *  Copy Constructor
   */
  SMA(const SMA &sma) : min(sma.getSMA_min()), max(sma.getSMA_max())  { assert(min <= max); }

  /**
   *  Copy Constructor
   */
  explicit SMA(SMA* sma) : min(sma->getSMA_min()), max(sma->getSMA_max())  { assert(min <= max); }

  /** 
   * Get the sma_min
   */
  inline uint64_t getSMA_min() const noexcept { return min; }

  /** 
   * Get the sma_max
   */
  inline uint64_t getSMA_max() const noexcept { return max; }

  /**
   * Get the sma min and max
   */
  inline std::pair<uint64_t, uint64_t> getSMA_min_max() const noexcept { return {min, max}; }

  /**
   * Set the sma_min
   */
  inline void setSMA_min(uint64_t min) noexcept { this->min = min; }

  /**
   * Set the sma_max
   */
  inline void setSMA_max(uint64_t max) noexcept { this->max = max; }

  operator std::string() const {
    std::stringstream ss;
    ss << "SMA: (min: " << getSMA_min() << ", max: " << getSMA_max() << ")";
    return ss.str();
  }

  bool operator==(SMA &sma2) const {
      return (this->min == sma2.getSMA_min() &&
              this->max == sma2.getSMA_max());
  }

  bool operator==(const SMA &sma2) const {
    return (this->min == sma2.getSMA_min() &&
            this->max == sma2.getSMA_max());
  }

  bool operator!= (const SMA &c2) {
    return !(*this == c2);
  }
};
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_DATABLOCK_SMA_H_
// ---------------------------------------------------------------------------------------------------

