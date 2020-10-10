// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_BUFFER_MANAGE_TID_H_
#define INCLUDE_IMLAB_BUFFER_MANAGE_TID_H_
// ---------------------------------------------------------------------------------------------------
#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
// ---------------------------------------------------------------------------------------------------
namespace imlab {
/**
 * TIDs (tuple identifier), consisting of a page ID and a slot ID
 * Page: a page
 * Slot: a data block as a data storage format contains multiple areas for attribute (tuple)
 * Samilar with NSM
 */
struct TID {
 public:
  /// Constructor.
  explicit TID(uint64_t raw_value) : value(raw_value) {}

  /// Constructor.
  TID(uint64_t page, uint16_t slot)
      : value((page << 16) ^ (slot & 0xFFFF)) {
    /// ^: Bitwise Exclusive OR Operator
  }

  /// Get buffer page id.
  uint64_t get_page_id(uint16_t segment_id) {
    return (value >> 16) ^ (static_cast<uint64_t>(segment_id) << 48);
  }

  /// Get the slot.
  uint16_t get_slot() { return value & 0xFFFF; }

  /// Get the value.
  uint64_t get_value() { return value; }

  /// Get buffer page id.
  uint64_t get_page_id(uint16_t segment_id) const {
    return (value >> 16) ^ (static_cast<uint64_t>(segment_id) << 48);
  }

  /// Get the slot.
  uint16_t get_slot() const { return value & 0xFFFF; }

  /// Get the value.
  uint64_t get_value() const { return value; }

  /**
   * Compare two TIDs
   */
  bool operator==(const TID& tid) const {
    return get_value() == tid.get_value();
  }

 private:
  /// The TID value.
  uint64_t value;
};
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
/**
 * Implement hash function for std::hash
 * We can use the TID value as hash (it uniquely identifies the TID)
 */
namespace std {
template <>
struct hash<imlab::TID> {
  std::size_t operator()(const imlab::TID &tid) const {
              return std::hash<uint64_t>()(tid.get_value());
  }
};
}
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_BUFFER_MANAGE_TID_H_
// ---------------------------------------------------------------------------------------------------

