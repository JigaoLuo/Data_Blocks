// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_DATABLOCK_PSMA_H_
#define INCLUDE_IMLAB_DATABLOCK_PSMA_H_
// ---------------------------------------------------------------------------------------------------
#include <cstdint>
#include <functional>
#include <sstream>
#include <tuple>
#include <vector>
#include <cstdlib>
// ---------------------------------------------------------------------------------------------------
namespace imlab {
/**
 *  "(v) Additionally, we include a novel light-weight index structure, PSMA,
 *  that maps a value to a range of positions on the Data Block where this value appears. 
 *  Using the PSMAs, scan ranges can further be narrowed, 
 *  even in cases where the value domain is large and entire blocks cannot be skipped"
 */
template <class T>
class PSMA {
 public:
  static constexpr size_t BYTE = sizeof(T);

  static constexpr uint32_t SIZE = BYTE * 0b0001'0000'0000;  // Number of entries

  // "For 2-byte delta values, 2^8 values map to the same table entry (2^16 for 3-byte values, and 2^24 for 4-byte values)."
  static constexpr uint32_t ENETY_VALUES = (BYTE - 1) * 0b0001'0000'0000;

  /**
   *  Compute the PSMA slot for a given value
   */
  static const uint32_t getPSMASlot(T value, T min) {
    uint64_t d = value - min;  // d = delta
    uint32_t r = d ? (7 - (__builtin_clzll(d) >> 3)) : 0;  // r = remaining bytes (note: clz is undefined for 0)
    uint64_t m = (d >> (r << 3));  // m = most significant non-zero byte
    return m + (r << 8);    // return the slot in PSMA array
  }

  /**
   * Build the lookup table.
   * "Building a lookup table is an O(n) operation. 
   * First, the table is initialized with empty ranges. 
   * Then, a scan over the column is performed. 
   *    For each value vi of the column C = (v0; : : : ; vn−1) the associated table entry is determined. 
   *    If the entry contains an empty range, then the entry is set to [i; i + 1), otherwise the range end is updated to i + 1."
   * 
   * @param basePtr the getData() from DataBlock as pointer
   * @param size the length of the data (pointer range)
   * @param min the min for getPSMASlot
   */
  void buildLookupTable(T* basePtr, size_t size, T min) {
    // Update ranges for all attribute values
    for (uint32_t tid = 0; tid != size; ++tid) {
      auto& entry = lookupTable[getPSMASlot(*(basePtr + tid), min)];
      auto& start = std::get<0>(entry);
      auto& end = std::get<1>(entry);
      if (start == 0 && end == 0) {  // if (entry.empty())
        // "entry={tid,tid+1};"
        start = tid;
        end = tid + 1;
      } else {
        end = tid + 1;  // entry.end=tid+1;
      }
    }
  }

  /**
   * "At query processing time, the potential range of tuples can now immediately be looked up in the PSMA"
   */
  const std::tuple<uint32_t, uint32_t> loopup(T value, T min) {
    auto range = lookupTable[getPSMASlot(value, min)];
    assert(std::get<0>(range) != std::get<1>(range));  // if =, means not found (not exist)
    return range;
  }

 private:
  std::tuple<uint32_t, uint32_t> lookupTable[SIZE];  // Initialize all slots to empty ranges
};
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_DATABLOCK_PSMA_H_
// ---------------------------------------------------------------------------------------------------

