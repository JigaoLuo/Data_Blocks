// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_BUFFER_MANAGE_PID_H_
#define INCLUDE_IMLAB_BUFFER_MANAGE_PID_H_
// -------------------------------------------------------------------------------------
#include <stdlib.h>
#include <sstream>
#include <string>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using std::string;
using std::stringstream;
// -------------------------------------------------------------------------------------
using ub1 = uint8_t;
using ub2 = uint16_t;
using ub8 = uint64_t;
// -------------------------------------------------------------------------------------
static constexpr size_t ub8_size = sizeof(ub8);
// -------------------------------------------------------------------------------------
namespace imlab {
// -------------------------------------------------------------------------------------
/**
 * Special class for page IDs. basically an invisible wrapper around uint64_t
 */
struct PID {
 private:
    // The 64 bits of a page id are divided into:
    // * A segment id in the most significant 16 bits
    // * segment page id in the least significant 48 bits.
    ub8 value;

    static constexpr ub8 mask = 281474976710655L;  // not allow to use 1 << 48 - 1 (not constexpr)

    static constexpr ub1 mask_digit = 48;

 public:
    PID(ub2 segment, ub8 page) : value((ub8(segment) << mask_digit) + page) {}

    explicit PID(ub8 value) : value(value) {}

    /** Returns the value of this PID (value = segmentId << 48 + pageId)
     * @return value of this PID in uint64_t
     */
    inline ub8 getValue() const {
        return value;
    }

    /** Returns the segmentId of this PID
     * @return segmentId of this PID in uint16_t
     */
    inline ub2 getSegmentId() const {
        return value >> mask_digit;
    }

    /** Returns the pageId of this PID
     * @return pageId of this PID in uint64_t
     */
    inline ub8 getPageId() const {
        return value & mask;
    }

    /** Returns a stirng
     * @return a string fo PID
     */
    operator string() const {
        stringstream ss;
        ss << "(segNr: " << getSegmentId() << ", pageNr: " << getPageId() << ")";
        return ss.str();
    }

    bool operator== (const PID& other) const {
        return value == other.value;
    }
};
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
namespace std {
template <>
struct hash<imlab::PID> {
    std::size_t operator()(const imlab::PID &tid) const {
        return std::hash<uint64_t>()(tid.getValue());
    }
};
}
// -------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_BUFFER_MANAGE_PID_H_
